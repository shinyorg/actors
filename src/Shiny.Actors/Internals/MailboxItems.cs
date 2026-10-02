using System.Collections.Immutable;
using System.Diagnostics;
using Shiny.Actors.Remoting;

namespace Shiny.Actors.Internals;


enum TurnMode { Exclusive, ReadOnly, Interleave }


abstract class MailboxItem
{
    protected MailboxItem()
    {
        // what the caller had - restored inside the turn so it flows on to anything the actor calls
        this.Context = ActorRequestContext.Snapshot;
        this.Parent = Activity.Current?.Context ?? default;
    }


    /// <summary>The caller's turn, for deadlock detection. Only asks carry one - a tell never waits.</summary>
    public CallChain? Chain { get; init; }

    /// <summary>The interface method being called; null for timers, stream events and reminders.</summary>
    public ActorMethod? Method { get; set; }

    /// <summary>The call's arguments, for filters - parallel to <see cref="ActorMethod.ParameterTypes"/>.</summary>
    public object?[]? Arguments { get; set; }

    public ImmutableDictionary<string, string>? Context { get; }

    public ActivityContext Parent { get; init; }

    /// <summary>Whether this item resets the idle clock.</summary>
    public virtual bool IsActivity => true;

    /// <summary>For telemetry: "ok", "error" or "cancelled" once executed.</summary>
    public string Outcome { get; protected set; } = "ok";

    public virtual string Label => this.Method?.Key ?? "turn";


    public TurnMode GetMode(ActorRegistration registration)
    {
        if (registration.IsReentrant)
            return TurnMode.Interleave;

        var flags = this.Method?.Flags ?? ActorMethodFlags.None;
        if ((flags & ActorMethodFlags.AlwaysInterleave) != 0)
            return TurnMode.Interleave;

        return (flags & ActorMethodFlags.ReadOnly) != 0 ? TurnMode.ReadOnly : TurnMode.Exclusive;
    }


    public abstract ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken);

    /// <summary>The actor could not run the item (activation failed, or the system shut down).</summary>
    public virtual void Fail(Exception exception) { }


    /// <summary>Runs the call through the filters (when there are any and this is a method call), then auto-save.</summary>
    protected async ValueTask<object?> InvokeAsync(ActorActivation activation, Func<Actor, CancellationToken, ValueTask<object?>> call, CancellationToken cancellationToken)
    {
        var actor = activation.Actor;
        object? result;

        if (this.Method is { } method && activation.HasFilters(actor))
        {
            var context = new ActorCallContext(actor, method, this.Arguments ?? [], cancellationToken);
            await activation.RunFiltersAsync(context, async c => c.Result = await call(c.Actor, c.CancellationToken).ConfigureAwait(false)).ConfigureAwait(false);
            result = context.Result;
        }
        else
        {
            result = await call(actor, cancellationToken).ConfigureAwait(false);
        }

        // a result is only handed back once it is saved
        await activation.AfterCallAsync().ConfigureAwait(false);
        return result;
    }
}


sealed class AskItem<TActor, TResult> : MailboxItem where TActor : class
{
    readonly Func<TActor, CancellationToken, ValueTask<TResult>> call;
    readonly CancellationToken cancellationToken;
    readonly TaskCompletionSource<TResult> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
    readonly CancellationTokenRegistration registration;


    public AskItem(Func<TActor, CancellationToken, ValueTask<TResult>> call, CancellationToken cancellationToken, CallChain? chain)
    {
        this.call = call;
        this.cancellationToken = cancellationToken;
        this.Chain = chain;

        // the caller stops waiting as soon as it cancels - even while queued behind other calls
        if (cancellationToken.CanBeCanceled)
            this.registration = cancellationToken.UnsafeRegister(
                static (s, ct) => ((TaskCompletionSource<TResult>)s!).TrySetCanceled(ct),
                this.tcs
            );
    }


    public Task<TResult> Task => this.tcs.Task;


    public override async ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken)
    {
        try
        {
            if (this.tcs.Task.IsCompleted)
            {
                this.Outcome = "cancelled"; // cancelled while queued
                return;
            }

            // the typed path stays unboxed unless a filter needs to see the result
            TResult result;
            if (this.Method is not null && activation.HasFilters(activation.Actor))
            {
                var boxed = await this.InvokeAsync(activation, async (a, ct) => await this.call((TActor)(object)a, ct).ConfigureAwait(false), this.cancellationToken).ConfigureAwait(false);
                result = this.Method.ReturnType is null || boxed is null ? default! : (TResult)boxed;
            }
            else
            {
                result = await this.call((TActor)(object)activation.Actor, this.cancellationToken).ConfigureAwait(false);
                await activation.AfterCallAsync().ConfigureAwait(false);
            }
            this.tcs.TrySetResult(result);
        }
        catch (OperationCanceledException) when (this.cancellationToken.IsCancellationRequested)
        {
            this.Outcome = "cancelled";
            this.tcs.TrySetCanceled(this.cancellationToken);
        }
        catch (Exception ex)
        {
            this.Outcome = "error";
            this.tcs.TrySetException(ex);
        }
        finally
        {
            this.registration.Dispose();
        }
    }


    public override void Fail(Exception exception)
    {
        this.tcs.TrySetException(exception);
        this.registration.Dispose();
    }
}


sealed class AskItem<TActor>(Func<TActor, CancellationToken, ValueTask> call, CancellationToken cancellationToken, CallChain? chain) : MailboxItem
    where TActor : class
{
    readonly AskItem<TActor, bool> inner = new(
        async (actor, ct) =>
        {
            await call(actor, ct).ConfigureAwait(false);
            return true;
        },
        cancellationToken,
        chain
    );

    public Task Task => this.inner.Task;

    public override async ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken)
    {
        // the inner item does the work, so it needs to know which method it is - filters key off it
        this.inner.Method = this.Method;
        this.inner.Arguments = this.Arguments;
        await this.inner.ExecuteAsync(activation, shutdownToken).ConfigureAwait(false);
        this.Outcome = this.inner.Outcome;
    }

    public override void Fail(Exception exception) => this.inner.Fail(exception);
}


/// <summary>Fire-and-forget. A failure is logged by the activation - there is no one to throw to.</summary>
sealed class TellItem<TActor>(Func<TActor, CancellationToken, ValueTask> call) : MailboxItem where TActor : class
{
    public override async ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken)
    {
        try
        {
            await this.InvokeAsync(activation, async (a, ct) =>
            {
                await call((TActor)(object)a, ct).ConfigureAwait(false);
                return null;
            }, shutdownToken).ConfigureAwait(false);
        }
        catch
        {
            this.Outcome = "error";
            throw;
        }
    }
}


/// <summary>An event for an actor's own stream subscription, or other system work run as a turn.</summary>
sealed class DelegateItem(Func<CancellationToken, ValueTask> call, string label = "stream") : MailboxItem
{
    public override string Label => label;

    public override async ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken)
    {
        try
        {
            await call(shutdownToken).ConfigureAwait(false);
            await activation.AfterCallAsync().ConfigureAwait(false);
        }
        catch
        {
            this.Outcome = "error";
            throw;
        }
    }
}


sealed class TimerItem(ActorTimer timer) : MailboxItem
{
    public override bool IsActivity => false;
    public override string Label => "timer";

    public override async ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken)
    {
        try
        {
            await timer.RunAsync(shutdownToken).ConfigureAwait(false);
            await activation.AfterCallAsync().ConfigureAwait(false);
        }
        catch
        {
            this.Outcome = "error";
            throw;
        }
    }

    public override void Fail(Exception exception) => timer.Release();
}


enum ControlKind { IdleCheck, Deactivate }

sealed class ControlItem(ControlKind kind, DeactivationReason reason = DeactivationReason.Requested) : MailboxItem
{
    public ControlKind Kind => kind;
    public DeactivationReason Reason => reason;
    public override bool IsActivity => false;
    public override ValueTask ExecuteAsync(ActorActivation activation, CancellationToken shutdownToken) => default;
}
