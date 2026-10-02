namespace Shiny.Actors.Internals;


sealed class ActorTimer(ActorActivation owner, Func<CancellationToken, ValueTask> callback) : IDisposable
{
    ITimer? timer;
    int pending;
    volatile bool disposed;


    public void Start(TimeSpan dueTime, TimeSpan period)
        => this.timer = owner.System.TimeProvider.CreateTimer(static s => ((ActorTimer)s!).Tick(), this, dueTime, period);


    void Tick()
    {
        if (this.disposed)
            return;

        // a slow callback must not let ticks pile up in the mailbox
        if (Interlocked.CompareExchange(ref this.pending, 1, 0) != 0)
            return;

        if (!owner.TryEnqueue(new TimerItem(this)))
            this.Release();
    }


    public async ValueTask RunAsync(CancellationToken cancellationToken)
    {
        try
        {
            if (!this.disposed)
                await callback(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.Release();
        }
    }


    public void Release() => Volatile.Write(ref this.pending, 0);


    public void Dispose()
    {
        if (this.disposed)
            return;

        this.disposed = true;
        this.timer?.Dispose();
        owner.RemoveTimer(this);
    }
}
