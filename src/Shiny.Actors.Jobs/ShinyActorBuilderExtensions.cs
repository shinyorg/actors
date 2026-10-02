using Shiny;
using Shiny.Actors;
using Shiny.Actors.Jobs;
using Shiny.Jobs;

namespace Microsoft.Extensions.DependencyInjection;


public static class JobsShinyActorBuilderExtensions
{
    /// <summary>
    /// Fires due reminders from the background through the OS scheduler (iOS BGTaskScheduler, Android WorkManager). The OS
    /// decides when - typically every 15 minutes or more - so a reminder due while the app isn't running fires on the
    /// next wake-up, not on the dot. Pair with <c>UseReminderNotifications()</c> for on-time alerts.
    /// </summary>
    /// <remarks>
    /// Use persistent storage - an in-memory reminder store is empty after the OS restarts the process to run the job.
    /// Follow the Shiny.Jobs platform setup (iOS Info.plist identifiers).
    /// </remarks>
    public static ShinyActorBuilder UseBackgroundReminders(this ShinyActorBuilder builder, Func<JobRegistration, JobRegistration>? configure = null)
    {
        builder.Services.AddJob<ActorReminderJob>(configure ?? (static r => r));
        return builder;
    }
}
