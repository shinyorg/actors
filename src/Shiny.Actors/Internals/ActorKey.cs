namespace Shiny.Actors.Internals;


// keyed by implementation, so one class answering for several contracts is one activation per id;
// Worker tells apart the parallel activations of a [StatelessWorker]
readonly record struct ActorKey(Type ImplementationType, string Id, int Worker = 0)
{
    public ActorKey(ActorRegistration registration, string id) : this(registration.ImplementationType, id) { }
}
