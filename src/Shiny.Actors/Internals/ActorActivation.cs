using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Shiny.Actors.Internals;


/// <summary>
/// One live actor: its instance, its mailbox, and the dispatcher that decides which queued call may run next.
/// </summary>
/// <remarks>
/// <para>
/// Every turn runs on this activation's exclusive scheduler, so the actor's code between awaits never overlaps,
/// whatever interleaves. The dispatcher then decides when a turn may <i>start</i>:
/// exclusive calls one at a time and alone; <see cref="ReadOnlyAttribute"/> calls together but never beside an
/// exclusive one; <see cref="AlwaysInterleaveAttribute"/> calls (and everything on a <see cref="ReentrantAttribute"/>
/// actor) straight away.
/// </para>
/// <para>
/// Lifecycle: created by GetOrAdd (cheap, nothing running) -> started on first post -> activated -> dispatching ->
/// stopping -> deactivated -> removed -> <see cref="Completion"/>. While stopping, a call made by one of this actor's
/// own running turns is still taken (refusing it would deadlock the shutdown); any other call is set aside and
/// re-posted, in order, to the next activation before anyone waiting on <see cref="Completion"/> moves on.
/// </para>
/// </remarks>
sealed class ActorActivation
{
    readonly Channel<MailboxItem> mailbox;
    readonly TaskCompletionSource completion = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly TaskScheduler scheduler = new ConcurrentExclusiveSchedulerPair().ExclusiveScheduler;
    readonly List<ActorTimer> timers = [];
    readonly List<IDisposable> subscriptions = [];
    readonly List<IActorStateBinding> states = [];
    readonly AutoSaveMode autoSave;
    readonly KeyValuePair<string, object?> actorTag;

    // dispatcher state - guarded by sync
    readonly Lock sync = new();
    readonly Queue<MailboxItem> pending = new();
    readonly List<MailboxItem> deferred = [];
    readonly HashSet<CallChain> running = [];
    readonly HashSet<CallChain> readers = [];
    CallChain? exclusiveTurn;
    TaskCompletionSource wake = new(TaskCreationOptions.RunContinuationsAsynchronously);

    bool constructed;
    volatile bool discardState;
    volatile bool stopping;
    long lastActivity;
    int reserved;
    int queued; // our own count - a single-reader channel cannot count itself
    int controlQueued;
    int started;
    int idleCheckQueued;
    DeactivationReason reason = DeactivationReason.Idle;
    Actor? actor;
    AsyncServiceScope? scope;


    /// <summary>The activation whose actor is being constructed on this thread - how states and Id reach a constructor.</summary>
    [ThreadStatic]
    internal static ActorActivation? Constructing;


    public ActorActivation(ActorSystem system, ActorRegistration registration, string id, int worker = 0)
    {
        this.System = system;
        this.Registration = registration;
        this.Id = id;
        this.Worker = worker;
        this.Logger = system.CreateLogger(registration);
        this.lastActivity = system.TimeProvider.GetTimestamp();
        this.autoSave = registration.AutoSave ?? system.Options.DefaultAutoSave;
        this.actorTag = new("actor", registration.Name);

        var capacity = system.Options.MailboxCapacity;
        this.mailbox = capacity > 0
            ? Channel.CreateBounded<MailboxItem>(new BoundedChannelOptions(capacity) { SingleReader = true, FullMode = BoundedChannelFullMode.Wait })
            : Channel.CreateUnbounded<MailboxItem>(new UnboundedChannelOptions { SingleReader = true });
    }


    public ActorSystem System { get; }
    public ActorRegistration Registration { get; }
    public string Id { get; }
    public int Worker { get; }
    public ILogger Logger { get; }
    public ActorKey Key => new(this.Registration.ImplementationType, this.Id, this.Worker);
    public Task Completion => this.completion.Task;
    public Actor Actor => this.actor ?? throw new InvalidOperationException("The actor is not activated.");

    public bool IsIdle
    {
        get
        {
            var timeout = this.System.Options.IdleTimeout;
            return timeout != Timeout.InfiniteTimeSpan
                && this.System.TimeProvider.GetElapsedTime(Interlocked.Read(ref this.lastActivity)) >= timeout;
        }
    }

    /// <summary>Calls queued or running - how a stateless worker pool picks the least busy.</summary>
    public int Load
    {
        get
        {
            lock (this.sync)
                return Volatile.Read(ref this.reserved) + Volatile.Read(ref this.queued) + this.running.Count + this.pending.Count;
        }
    }


    /// <summary>
    /// Counts a call against this worker from the moment it is picked, so callers racing in at the same instant
    /// spread out instead of all seeing an empty mailbox.
    /// </summary>
    public void Reserve() => Interlocked.Increment(ref this.reserved);

    public void Unreserve() => Interlocked.Decrement(ref this.reserved);

    public override string ToString() => this.Worker == 0 ? $"{this.Registration.Name}/{this.Id}" : $"{this.Registration.Name}/{this.Id}#{this.Worker}";


    #region Posting

    /// <summary>Returns false when this activation has stopped taking items - wait for <see cref="Completion"/> and retry.</summary>
    public async ValueTask<bool> TryPostAsync(MailboxItem item, CancellationToken cancellationToken)
    {
        this.EnsureStarted();
        if (item.IsActivity)
            Interlocked.Exchange(ref this.lastActivity, this.System.TimeProvider.GetTimestamp());

        var writer = this.mailbox.Writer;
        if (!writer.TryWrite(item))
        {
            var written = false;
            while (!written && await writer.WaitToWriteAsync(cancellationToken).ConfigureAwait(false))
                written = writer.TryWrite(item);

            if (!written)
                return false;
        }

        if (item is ControlItem)
        {
            Interlocked.Increment(ref this.controlQueued);
        }
        else
        {
            Interlocked.Increment(ref this.queued);
            ActorTelemetry.Queued.Add(1, this.actorTag);
        }
        return true;
    }


    /// <summary>Non-blocking post for system traffic (timer ticks, idle checks). Never starts an activation.</summary>
    public bool TryEnqueue(MailboxItem item)
    {
        if (Volatile.Read(ref this.started) != 1)
            return false;

        if (item is ControlItem)
            Interlocked.Increment(ref this.controlQueued);
        if (this.mailbox.Writer.TryWrite(item))
            return true;

        if (item is ControlItem)
            Interlocked.Decrement(ref this.controlQueued);
        return false;
    }


    /// <summary>Nothing queued (not even an idle check), nothing running, not on its way out.</summary>
    public bool IsSettled => this.Load == 0 && Volatile.Read(ref this.controlQueued) == 0 && !this.stopping;


    /// <summary>
    /// Whether <paramref name="item"/> could never start because its caller is (transitively) waiting on a turn that
    /// blocks it - the caller would hang forever.
    /// </summary>
    public bool WouldDeadlock(MailboxItem item)
    {
        if (item.Chain is not { } chain)
            return false;

        var mode = item.GetMode(this.Registration);
        if (mode == TurnMode.Interleave)
            return false;

        lock (this.sync)
        {
            if (this.exclusiveTurn is { } exclusive && chain.Contains(exclusive))
                return true;

            return mode == TurnMode.Exclusive && this.readers.Any(chain.Contains);
        }
    }


    public void QueueIdleCheck()
    {
        if (Interlocked.CompareExchange(ref this.idleCheckQueued, 1, 0) == 0 && !this.TryEnqueue(new ControlItem(ControlKind.IdleCheck)))
            Volatile.Write(ref this.idleCheckQueued, 0);
    }


    public async Task StopAsync(DeactivationReason why)
    {
        // if it never started there is nothing to drain - but it must still leave the system
        await this.TryPostAsync(new ControlItem(ControlKind.Deactivate, why), CancellationToken.None).ConfigureAwait(false);
        await this.Completion.ConfigureAwait(false);
    }


    /// <param name="discardState">The in-memory state is stale (an ETag conflict) - skip auto-save on the way out.</param>
    public void RequestDeactivation(bool discardState = false)
    {
        if (discardState)
            this.discardState = true;

        this.Stop(DeactivationReason.Requested);
    }


    void Stop(DeactivationReason why)
    {
        lock (this.sync)
        {
            if (this.stopping)
                return;

            this.stopping = true;
            this.reason = why;
            this.wake.TrySetResult();
        }
    }


    void EnsureStarted()
    {
        if (Interlocked.Exchange(ref this.started, 1) != 0)
            return;

        // the loop must not inherit the first caller's async-locals (its call chain especially)
        using (ExecutionContext.SuppressFlow())
            _ = Task.Run(this.RunAsync);
    }

    #endregion


    #region Dispatch

    async Task RunAsync()
    {
        try
        {
            try
            {
                await this.ActivateAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                this.Logger.LogError(ex, "Actor {Actor} failed to activate", this);
                this.stopping = true;
                this.mailbox.Writer.TryComplete();
                this.FailPending(ex);
                await this.DisposeInstanceAsync().ConfigureAwait(false);
                return;
            }

            await this.DispatchAsync().ConfigureAwait(false);
            await this.DeactivateAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            // nothing above should throw - this is a last line so Completion always fires
            this.Logger.LogCritical(ex, "Actor {Actor} loop faulted", this);
        }
        finally
        {
            this.System.Remove(this);
            await this.RepostDeferredAsync().ConfigureAwait(false);
            this.completion.TrySetResult();
        }
    }


    async Task DispatchAsync()
    {
        var reader = this.mailbox.Reader;
        Task<bool>? mailWait = null;
        var mailOpen = true;

        while (true)
        {
            Task signal;
            lock (this.sync)
            {
                // a fresh signal before looking at state, so a turn finishing from here on is never missed
                this.wake = new(TaskCreationOptions.RunContinuationsAsynchronously);
                signal = this.wake.Task;
            }

            lock (this.sync)
            {
                // taken off the mailbox and queued in one step, so Load never sees a call in neither place
                while (reader.TryRead(out var item))
                    this.Accept(item);

                this.Pump();

                var quiet = this.running.Count == 0 && this.pending.Count == 0;
                if (this.stopping && quiet)
                {
                    // nothing of ours is running, so nothing can still call us: close, and set aside what's left
                    this.mailbox.Writer.TryComplete();
                    while (reader.TryRead(out var late))
                    {
                        this.Dequeued(late);
                        this.Defer(late);
                    }

                    return;
                }
                if (!mailOpen && quiet)
                    return;
            }

            if (mailOpen)
            {
                mailWait ??= reader.WaitToReadAsync().AsTask();
                if (await Task.WhenAny(mailWait, signal).ConfigureAwait(false) == mailWait)
                {
                    mailOpen = await mailWait.ConfigureAwait(false);
                    mailWait = null;
                }
            }
            else
            {
                await signal.ConfigureAwait(false);
            }
        }
    }


    void Accept(MailboxItem item)
    {
        this.Dequeued(item);
        if (item is ControlItem control)
        {
            this.HandleControl(control);
            return;
        }

        lock (this.sync)
        {
            if (this.stopping && !this.IsOwnCall(item))
            {
                this.Defer(item);
                return;
            }

            if (item.GetMode(this.Registration) == TurnMode.Interleave)
                this.StartTurn(item, TurnMode.Interleave);
            else
                this.pending.Enqueue(item);
        }
    }


    // under sync: start whatever the head of the queue allows
    void Pump()
    {
        while (this.pending.TryPeek(out var next))
        {
            if (this.exclusiveTurn is not null)
                return;

            var mode = next.GetMode(this.Registration);
            if (mode == TurnMode.Exclusive && this.readers.Count > 0)
                return;

            this.pending.Dequeue();
            this.StartTurn(next, mode);

            if (mode == TurnMode.Exclusive)
                return;
        }
    }


    // under sync
    void StartTurn(MailboxItem item, TurnMode mode)
    {
        var node = new CallChain(this, item.Chain);
        this.running.Add(node);
        if (mode == TurnMode.Exclusive)
            this.exclusiveTurn = node;
        else if (mode == TurnMode.ReadOnly)
            this.readers.Add(node);

        Task.Factory
            .StartNew(() => this.RunTurnAsync(item, node), CancellationToken.None, TaskCreationOptions.DenyChildAttach, this.scheduler)
            .Unwrap()
            .ContinueWith(
                static (_, s) =>
                {
                    var (activation, finished) = ((ActorActivation, CallChain))s!;
                    activation.TurnFinished(finished);
                },
                (this, node),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default
            );
    }


    void TurnFinished(CallChain node)
    {
        lock (this.sync)
        {
            this.running.Remove(node);
            this.readers.Remove(node);
            if (ReferenceEquals(this.exclusiveTurn, node))
                this.exclusiveTurn = null;

            this.wake.TrySetResult();
        }
    }


    // under sync: a call made from inside one of this activation's running turns
    bool IsOwnCall(MailboxItem item)
    {
        for (var c = item.Chain; c is not null; c = c.Parent)
            if (this.running.Contains(c))
                return true;

        return false;
    }


    void Dequeued(MailboxItem item)
    {
        if (item is ControlItem)
        {
            Interlocked.Decrement(ref this.controlQueued);
            return;
        }

        Interlocked.Decrement(ref this.queued);
        ActorTelemetry.Queued.Add(-1, this.actorTag);
    }


    void Defer(MailboxItem item)
    {
        if (item is not ControlItem)
            this.deferred.Add(item);
    }


    async Task RepostDeferredAsync()
    {
        if (this.deferred.Count == 0)
            return;

        // in their original order, and before anyone waiting on Completion can overtake them
        foreach (var item in this.deferred)
        {
            try
            {
                await this.System.PostAsync(this.Registration, this.Id, item, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                item.Fail(ex);
            }
        }
        this.deferred.Clear();
    }


    void HandleControl(ControlItem control)
    {
        if (control.Kind == ControlKind.Deactivate)
        {
            this.Stop(control.Reason);
            return;
        }

        Volatile.Write(ref this.idleCheckQueued, 0);
        bool busy;
        lock (this.sync)
            busy = this.running.Count > 0 || this.pending.Count > 0;

        if (!busy && this.IsIdle)
            this.Stop(DeactivationReason.Idle);
    }


    async Task RunTurnAsync(MailboxItem item, CallChain node)
    {
        // async-local changes made here revert when the turn ends, so each turn starts clean
        CallChain.Current = node;
        ActorRequestContext.Restore(item.Context);

        var method = item.Method;
        using var activity = method is null
            ? null
            : ActorTelemetry.StartCall(method.Contract.Name, method.Name, this.Registration.Name, this.Id, ActivityKind.Internal, item.Parent);
        var startedAt = Stopwatch.GetTimestamp();

        try
        {
            await item.ExecuteAsync(this, this.System.ShutdownToken).ConfigureAwait(false);
            if (item.Outcome == "error")
                activity?.SetStatus(ActivityStatusCode.Error);
        }
        catch (Exception ex)
        {
            // asks report to their caller; only one-way work reaches here
            activity.Failed(ex);
            this.Logger.LogError(ex, "Actor {Actor} failed processing a one-way call ({Label})", this, item.Label);
        }
        finally
        {
            var tags = new TagList { this.actorTag, { "method", item.Label }, { "outcome", item.Outcome } };
            ActorTelemetry.Calls.Add(1, tags);
            ActorTelemetry.CallDuration.Record(Stopwatch.GetElapsedTime(startedAt).TotalMilliseconds, tags);
        }
    }

    #endregion


    #region Lifecycle

    async Task ActivateAsync()
    {
        var services = this.System.Services;
        if (services.GetService(typeof(IServiceScopeFactory)) is IServiceScopeFactory factory)
        {
            this.scope = factory.CreateAsyncScope();
            services = this.scope.Value.ServiceProvider;
        }

        Constructing = this;
        try
        {
            this.actor = this.Registration.Factory(services);
        }
        finally
        {
            Constructing = null;
            this.constructed = true;
        }
        this.actor.Attach(this);

        await this.RunOnSchedulerAsync(async ct =>
        {
            foreach (var state in this.states)
                await state.ReadStateAsync(ct).ConfigureAwait(false);

            await this.actor.OnActivateAsync(ct).ConfigureAwait(false);
        }, this.System.ShutdownToken).ConfigureAwait(false);

        ActorTelemetry.Activations.Add(1, this.actorTag);
        ActorTelemetry.Active.Add(1, this.actorTag);
        this.Logger.LogDebug("Actor {Actor} activated", this);
    }


    async Task DeactivateAsync()
    {
        lock (this.timers)
        {
            foreach (var timer in this.timers.ToArray())
                timer.Dispose();
        }
        lock (this.subscriptions)
        {
            foreach (var sub in this.subscriptions)
                sub.Dispose();
            this.subscriptions.Clear();
        }

        try
        {
            using var cts = new CancellationTokenSource(this.System.Options.DeactivationTimeout, this.System.TimeProvider);
            await this.RunOnSchedulerAsync(ct => this.actor!.OnDeactivateAsync(this.reason, ct), cts.Token).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.Logger.LogError(ex, "Actor {Actor} failed in OnDeactivateAsync", this);
        }

        if (this.autoSave != AutoSaveMode.None && !this.discardState)
        {
            try
            {
                await this.SaveChangedStatesAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                this.Logger.LogError(ex, "Actor {Actor} failed to save its state while deactivating", this);
            }
        }

        await this.DisposeInstanceAsync().ConfigureAwait(false);
        ActorTelemetry.Active.Add(-1, this.actorTag);
        ActorTelemetry.Deactivations.Add(1, this.actorTag, new("reason", this.reason.ToString()));
        this.Logger.LogDebug("Actor {Actor} deactivated ({Reason})", this, this.reason);
    }


    // activation and deactivation run as turns of their own, on the actor's scheduler like every other turn
    async ValueTask RunOnSchedulerAsync(Func<CancellationToken, ValueTask> work, CancellationToken cancellationToken)
    {
        var node = new CallChain(this, null);
        lock (this.sync)
        {
            this.running.Add(node);
            this.exclusiveTurn = node;
        }

        try
        {
            await Task.Factory.StartNew(async () =>
            {
                CallChain.Current = node;
                await work(cancellationToken).ConfigureAwait(false);
            }, CancellationToken.None, TaskCreationOptions.DenyChildAttach, this.scheduler).Unwrap().ConfigureAwait(false);
        }
        finally
        {
            lock (this.sync)
            {
                this.running.Remove(node);
                this.exclusiveTurn = null;
            }
        }
    }


    async Task DisposeInstanceAsync()
    {
        try
        {
            switch (this.actor)
            {
                case IAsyncDisposable ad: await ad.DisposeAsync().ConfigureAwait(false); break;
                case IDisposable d: d.Dispose(); break;
            }
            if (this.scope is { } s)
                await s.DisposeAsync().ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            this.Logger.LogError(ex, "Actor {Actor} failed to dispose", this);
        }
    }


    void FailPending(Exception ex)
    {
        var error = new InvalidOperationException($"Actor '{this}' failed to activate: {ex.Message}", ex);
        while (this.mailbox.Reader.TryRead(out var item))
        {
            this.Dequeued(item);
            item.Fail(error);
        }
    }

    #endregion


    #region Turn services

    public bool HasFilters(Actor instance) => this.System.CallFilters.Length > 0 || instance is IActorCallFilter;


    public ValueTask RunFiltersAsync(ActorCallContext context, ActorCallDelegate terminal)
    {
        var filters = this.System.CallFilters;
        var own = context.Actor as IActorCallFilter;

        ValueTask Next(int index, ActorCallContext ctx)
        {
            if (index < filters.Length)
                return filters[index].InvokeAsync(ctx, c => Next(index + 1, c));

            // the actor's own filter is innermost
            return index == filters.Length && own is not null
                ? own.InvokeAsync(ctx, c => terminal(c))
                : terminal(ctx);
        }

        return Next(0, context);
    }


    public ValueTask AfterCallAsync() => this.autoSave == AutoSaveMode.AfterEachCall ? this.SaveChangedStatesAsync() : default;


    public TState AddState<TState>(TState state) where TState : IActorStateBinding
    {
        if (this.constructed)
            throw new InvalidOperationException("Actor states must be created while the actor is constructed (constructor or [ActorState] parameter).");
        if (this.Registration.IsStatelessWorker)
            throw new InvalidOperationException($"'{this.Registration.Name}' is a [StatelessWorker] - several may run per id, so it cannot have persistent state.");
        if (this.states.Any(x => x.Name == state.Name))
            throw new InvalidOperationException($"Actor '{this}' already has a state named '{state.Name}'.");

        this.states.Add(state);
        return state;
    }


    ValueTask SaveChangedStatesAsync() => this.states.Count switch
    {
        0 => default,
        1 => this.states[0].SaveIfChangedAsync(CancellationToken.None),
        _ => SaveAll(this.states)
    };


    static async ValueTask SaveAll(List<IActorStateBinding> states)
    {
        foreach (var state in states)
            await state.SaveIfChangedAsync(CancellationToken.None).ConfigureAwait(false);
    }


    public IDisposable RegisterTimer(Func<CancellationToken, ValueTask> callback, TimeSpan dueTime, TimeSpan period)
    {
        this.EnsureInsideTurn(nameof(RegisterTimer));
        var timer = new ActorTimer(this, callback);
        lock (this.timers)
            this.timers.Add(timer);

        timer.Start(dueTime, period);
        return timer;
    }


    public void RemoveTimer(ActorTimer timer)
    {
        lock (this.timers)
            this.timers.Remove(timer);
    }


    public void TrackSubscription(IDisposable subscription)
    {
        lock (this.subscriptions)
            this.subscriptions.Add(subscription);
    }


    public void UntrackSubscription(IDisposable subscription)
    {
        lock (this.subscriptions)
            this.subscriptions.Remove(subscription);
    }


    void EnsureInsideTurn(string member)
    {
        if (CallChain.Current?.Activation != this)
            throw new InvalidOperationException($"{member} must be called from inside the actor ('{this}'), e.g. from OnActivateAsync or one of its methods.");
    }

    #endregion
}
