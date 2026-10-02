using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace Shiny.Actors.Internals;


/// <summary>
/// Keeps the reminder table in memory, persists every change, and fires what is due - from a timer armed
/// for the next due reminder while the process runs, or from <see cref="RunDueAsync"/> when a background
/// job wakes the app. Delivery is at least once: a reminder only moves on after its actor handled it.
/// </summary>
sealed class ActorReminderService : IAsyncDisposable
{
    static readonly TimeSpan MaxTimerDelay = TimeSpan.FromHours(1);

    readonly ActorSystem system;
    readonly IActorReminderStore store;
    readonly ILogger logger;
    readonly SemaphoreSlim gate = new(1, 1);
    readonly SemaphoreSlim firing = new(1, 1);
    readonly ITimer timer;
    ConcurrentDictionary<(string, string, string), ActorReminder>? reminders;
    volatile bool disposed;
    int inFlight; // timer fired, run not finished - quiet detection must not slip through the gap


    readonly IActorReminderObserver[] observers;


    public ActorReminderService(ActorSystem system, IActorReminderStore store, IActorReminderObserver[] observers, ILogger logger)
    {
        this.system = system;
        this.store = store;
        this.observers = observers;
        this.logger = logger;
        this.timer = system.TimeProvider.CreateTimer(static s => ((ActorReminderService)s!).OnTimer(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
    }


    /// <summary>Loads persisted reminders and arms the timer, so reminders fire without any actor being touched.</summary>
    public void Start()
    {
        using (ExecutionContext.SuppressFlow())
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    var loaded = await this.LockedAsync(all => Task.FromResult(all.Values.ToList()), CancellationToken.None).ConfigureAwait(false);
                    this.Arm();

                    // the OS schedule may be gone (reinstall, cleared notifications) - bring it back in line
                    foreach (var reminder in loaded)
                        await this.NotifyAsync(reminder, removed: false).ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Failed to load actor reminders");
                }
            });
        }
    }


    public async ValueTask RegisterAsync(ActorReminder reminder, CancellationToken cancellationToken)
    {
        await this.LockedAsync(async all =>
        {
            await this.store.SaveAsync(reminder, cancellationToken).ConfigureAwait(false);
            all[reminder.Key] = reminder;
            return true;
        }, cancellationToken).ConfigureAwait(false);
        this.Arm();
        await this.NotifyAsync(reminder, removed: false).ConfigureAwait(false);
    }


    public async ValueTask<bool> UnregisterAsync(string actorName, string actorId, string name, CancellationToken cancellationToken)
    {
        var removed = await this.LockedAsync<ActorReminder?>(async all =>
        {
            if (!all.TryRemove((actorName, actorId, name), out var existing))
                return null;

            await this.store.RemoveAsync(existing, cancellationToken).ConfigureAwait(false);
            return existing;
        }, cancellationToken).ConfigureAwait(false);

        if (removed is null)
            return false;

        await this.NotifyAsync(removed, removed: true).ConfigureAwait(false);
        return true;
    }


    async ValueTask NotifyAsync(ActorReminder reminder, bool removed)
    {
        foreach (var observer in this.observers)
        {
            try
            {
                if (removed)
                    await observer.OnRemovedAsync(reminder, CancellationToken.None).ConfigureAwait(false);
                else
                    await observer.OnScheduledAsync(reminder, CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                this.logger.LogWarning(ex, "Reminder observer {Observer} failed for {Reminder}", observer.GetType().Name, reminder.Name);
            }
        }
    }


    public ValueTask<IReadOnlyList<ActorReminder>> GetAsync(string actorName, string actorId, CancellationToken cancellationToken)
        => this.LockedAsync<IReadOnlyList<ActorReminder>>(
            all => Task.FromResult<IReadOnlyList<ActorReminder>>([.. all.Values.Where(x => x.ActorName == actorName && x.ActorId == actorId)]),
            cancellationToken
        );


    /// <summary>Fires everything due now. Returns how many reminders were delivered.</summary>
    /// <param name="deactivateAfter">
    /// Deactivate actors that were not active before this run once their reminder is handled - for a background
    /// wake-up, where the OS may suspend the process the moment the job returns.
    /// </param>
    public async Task<int> RunDueAsync(bool deactivateAfter, CancellationToken cancellationToken)
    {
        Interlocked.Increment(ref this.inFlight);
        try
        {
            return await this.RunDueCoreAsync(deactivateAfter, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            Interlocked.Decrement(ref this.inFlight);
        }
    }


    async Task<int> RunDueCoreAsync(bool deactivateAfter, CancellationToken cancellationToken)
    {
        // one run at a time - the timer and a background job can overlap
        await this.firing.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var now = this.system.TimeProvider.GetUtcNow();
            var due = await this.LockedAsync<List<ActorReminder>>(
                all => Task.FromResult(all.Values.Where(x => x.DueAt <= now).ToList()),
                cancellationToken
            ).ConfigureAwait(false);

            var results = await Task.WhenAll(due.Select(r => this.FireAsync(r, now, deactivateAfter, cancellationToken))).ConfigureAwait(false);
            return results.Count(x => x);
        }
        finally
        {
            this.firing.Release();
            this.Arm();
        }
    }


    async Task<bool> FireAsync(ActorReminder reminder, DateTimeOffset now, bool deactivateAfter, CancellationToken cancellationToken)
    {
        var registration = this.system.FindRegistrationByName(reminder.ActorName);
        var delivered = false;

        if (registration is null)
        {
            // most likely an assembly that has not loaded yet (see ActorSystemOptions.AddAssembly) - keep it
            this.logger.LogWarning("Reminder {Reminder} is for actor '{Actor}', which is not registered", reminder.Name, reminder.ActorName);
        }
        else
        {
            var wasActive = this.system.IsActive(registration, reminder.ActorId);
            try
            {
                var tick = new ReminderTick(reminder.Name, reminder.DueAt, now, reminder.Period);
                var item = new AskItem<IRemindable>((actor, ct) => actor.ReceiveReminderAsync(tick, ct), cancellationToken, null);
                await this.system.PostAsync(registration, reminder.ActorId, item, cancellationToken).ConfigureAwait(false);
                await item.Task.ConfigureAwait(false);
                delivered = true;
            }
            catch (Exception ex) when (!cancellationToken.IsCancellationRequested)
            {
                this.logger.LogError(ex, "Reminder {Reminder} for {Actor}/{Id} failed - it will be retried", reminder.Name, reminder.ActorName, reminder.ActorId);
            }

            if (deactivateAfter && !wasActive)
                await this.system.DeactivateAsync(registration, reminder.ActorId).ConfigureAwait(false);
        }

        var outcome = await this.LockedAsync<(ActorReminder Reminder, bool Removed)?>(async all =>
        {
            // unregistered or re-registered while it was firing - the newer intent wins
            if (!all.TryGetValue(reminder.Key, out var current) || current != reminder)
                return null;

            ActorReminder? next = (delivered, reminder.Period) switch
            {
                (true, null) => null,
                (true, { } period) => reminder with { DueAt = NextDue(reminder.DueAt, period, now) },
                (false, var period) => reminder with { DueAt = now + Min(period ?? TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1)) }
            };

            if (next is null)
            {
                all.TryRemove(reminder.Key, out _);
                await this.store.RemoveAsync(reminder, CancellationToken.None).ConfigureAwait(false);
                return (reminder, true);
            }

            all[reminder.Key] = next;
            await this.store.SaveAsync(next, CancellationToken.None).ConfigureAwait(false);
            return (next, false);
        }, CancellationToken.None).ConfigureAwait(false);

        if (outcome is { } changed)
            await this.NotifyAsync(changed.Reminder, changed.Removed).ConfigureAwait(false);

        if (delivered)
            ActorTelemetry.RemindersFired.Add(1, new KeyValuePair<string, object?>("actor", reminder.ActorName));
        return delivered;
    }


    // missed periods (the app was not running) collapse into the one tick just delivered
    static DateTimeOffset NextDue(DateTimeOffset dueAt, TimeSpan period, DateTimeOffset now)
    {
        if (dueAt + period > now)
            return dueAt + period;

        var missed = (now - dueAt).Ticks / period.Ticks;
        return dueAt + TimeSpan.FromTicks(period.Ticks * (missed + 1));
    }


    static TimeSpan Min(TimeSpan a, TimeSpan b) => a < b ? a : b;


    /// <summary>A reminder run is queued or in progress.</summary>
    public bool IsBusy => Volatile.Read(ref this.inFlight) > 0;


    void OnTimer()
    {
        if (this.disposed)
            return;

        Interlocked.Increment(ref this.inFlight);
        using (ExecutionContext.SuppressFlow())
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await this.RunDueAsync(false, this.system.ShutdownToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException) { }
                catch (Exception ex)
                {
                    this.logger.LogError(ex, "Failed to run due reminders");
                }
                finally
                {
                    Interlocked.Decrement(ref this.inFlight);
                }
            });
        }
    }


    void Arm()
    {
        if (this.disposed || this.reminders is null)
            return;

        var snapshot = this.reminders.Values; // a point-in-time copy
        DateTimeOffset? next = snapshot.Count == 0 ? null : snapshot.Min(x => x.DueAt);

        if (next is null)
        {
            this.timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
            return;
        }

        var delay = next.Value - this.system.TimeProvider.GetUtcNow();
        if (delay < TimeSpan.Zero)
            delay = TimeSpan.Zero;
        else if (delay > MaxTimerDelay)
            delay = MaxTimerDelay; // re-armed when it fires; keeps far-off reminders honest across clock changes

        this.timer.Change(delay, Timeout.InfiniteTimeSpan);
    }


    async ValueTask<T> LockedAsync<T>(Func<ConcurrentDictionary<(string, string, string), ActorReminder>, Task<T>> work, CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            // writers are serialized by the gate; Arm reads without it, hence the concurrent dictionary
            if (this.reminders is null)
            {
                var stored = await this.store.GetAllAsync(cancellationToken).ConfigureAwait(false);
                this.reminders = new(stored.Select(x => KeyValuePair.Create(x.Key, x)));
            }
            return await work(this.reminders).ConfigureAwait(false);
        }
        finally
        {
            this.gate.Release();
        }
    }


    public async ValueTask DisposeAsync()
    {
        this.disposed = true;
        await this.timer.DisposeAsync().ConfigureAwait(false);
        await this.firing.WaitAsync().ConfigureAwait(false);
        this.firing.Release();
    }
}
