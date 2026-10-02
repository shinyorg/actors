using Microsoft.Extensions.Logging;
using Shiny.Jobs;

namespace Shiny.Actors.Jobs;


/// <summary>
/// Fires every due actor reminder, then deactivates the actors it woke so their state is written before the OS
/// suspends the app again. While the app is in the foreground the actor system fires reminders on its own timer.
/// </summary>
public sealed class ActorReminderJob(ActorSystem actors, ILogger<ActorReminderJob> logger) : IJob
{
    public async Task Run(CancellationToken cancelToken)
    {
        var fired = await actors.RunDueRemindersAsync(deactivateAfter: true, cancelToken).ConfigureAwait(false);
        if (fired > 0)
            logger.LogInformation("Fired {Count} actor reminder(s) from the background", fired);
    }
}
