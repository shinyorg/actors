using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace Shiny.Actors;


/// <summary>
/// Traces and metrics, both named <c>Shiny.Actors</c>. With OpenTelemetry:
/// <c>.WithTracing(t => t.AddSource(ActorTelemetry.Name)).WithMetrics(m => m.AddMeter(ActorTelemetry.Name))</c>.
/// Nothing is recorded unless something listens.
/// </summary>
/// <remarks>
/// <para>Traces: one span per actor call, named <c>{interface}/{method}</c>, parented to the caller's span - also
/// across a remote call (W3C <c>traceparent</c>).</para>
/// <para>Metrics (actor ids are never used as tags - they are unbounded):</para>
/// <list type="bullet">
/// <item><c>shiny.actors.calls</c> / <c>shiny.actors.call.duration</c> (ms) - tagged actor, method, outcome</item>
/// <item><c>shiny.actors.activations.active</c>, <c>shiny.actors.activations</c>, <c>shiny.actors.deactivations</c> (reason)</item>
/// <item><c>shiny.actors.mailbox.queued</c> - calls waiting, across all actors</item>
/// <item><c>shiny.actors.state.writes</c>, <c>shiny.actors.state.write.duration</c> (ms), <c>shiny.actors.state.conflicts</c></item>
/// <item><c>shiny.actors.reminders.fired</c>, <c>shiny.actors.stream.events</c></item>
/// </list>
/// </remarks>
public static class ActorTelemetry
{
    public const string Name = "Shiny.Actors";

    static readonly string? Version = typeof(ActorTelemetry).Assembly.GetName().Version?.ToString();

    public static readonly ActivitySource ActivitySource = new(Name, Version);
    public static readonly Meter Meter = new(Name, Version);

    internal static readonly Counter<long> Calls = Meter.CreateCounter<long>("shiny.actors.calls", "{call}", "Actor calls handled");
    internal static readonly Histogram<double> CallDuration = Meter.CreateHistogram<double>("shiny.actors.call.duration", "ms", "Time an actor spent handling a call");
    internal static readonly UpDownCounter<long> Active = Meter.CreateUpDownCounter<long>("shiny.actors.activations.active", "{actor}", "Actors active now");
    internal static readonly Counter<long> Activations = Meter.CreateCounter<long>("shiny.actors.activations", "{actor}", "Actors activated");
    internal static readonly Counter<long> Deactivations = Meter.CreateCounter<long>("shiny.actors.deactivations", "{actor}", "Actors deactivated");
    internal static readonly UpDownCounter<long> Queued = Meter.CreateUpDownCounter<long>("shiny.actors.mailbox.queued", "{call}", "Calls waiting in mailboxes");
    internal static readonly Counter<long> StateWrites = Meter.CreateCounter<long>("shiny.actors.state.writes", "{write}", "State writes");
    internal static readonly Histogram<double> StateWriteDuration = Meter.CreateHistogram<double>("shiny.actors.state.write.duration", "ms", "Time to write actor state");
    internal static readonly Counter<long> StateConflicts = Meter.CreateCounter<long>("shiny.actors.state.conflicts", "{conflict}", "State writes refused for a stale ETag");
    internal static readonly Counter<long> RemindersFired = Meter.CreateCounter<long>("shiny.actors.reminders.fired", "{reminder}", "Reminders delivered");
    internal static readonly Counter<long> StreamEvents = Meter.CreateCounter<long>("shiny.actors.stream.events", "{event}", "Stream events published");


    internal static Activity? StartCall(string contract, string method, string actorName, string actorId, ActivityKind kind, ActivityContext parent)
    {
        if (!ActivitySource.HasListeners())
            return null;

        var activity = ActivitySource.StartActivity($"{contract}/{method}", kind, parent);
        if (activity is { IsAllDataRequested: true })
        {
            activity.SetTag("rpc.system", "shiny.actors");
            activity.SetTag("rpc.service", contract);
            activity.SetTag("rpc.method", method);
            activity.SetTag("actor.name", actorName);
            activity.SetTag("actor.id", actorId);
        }
        return activity;
    }


    internal static void Failed(this Activity? activity, Exception exception)
    {
        if (activity is null)
            return;

        activity.SetStatus(ActivityStatusCode.Error, exception.Message);
        activity.AddException(exception);
    }
}
