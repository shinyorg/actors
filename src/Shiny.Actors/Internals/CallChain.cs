using System.Text;

namespace Shiny.Actors.Internals;


/// <summary>
/// One node per turn in progress, linked back through the asks that led to it. A caller whose chain
/// contains the target's current turn is waiting on itself.
/// </summary>
sealed class CallChain(ActorActivation activation, CallChain? parent)
{
    static readonly AsyncLocal<CallChain?> current = new();

    public static CallChain? Current
    {
        get => current.Value;
        set => current.Value = value;
    }

    public ActorActivation Activation => activation;
    public CallChain? Parent => parent;


    public bool Contains(CallChain? node)
    {
        if (node is null)
            return false;

        for (var c = this; c is not null; c = c.Parent)
            if (ReferenceEquals(c, node))
                return true;

        return false;
    }


    public string Describe(ActorActivation target)
    {
        var path = new List<string> { target.ToString() };
        for (var c = this; c is not null; c = c.Parent)
            path.Add(c.Activation.ToString());

        path.Reverse();
        return string.Join(" -> ", path);
    }
}
