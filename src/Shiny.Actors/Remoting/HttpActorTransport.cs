using System.Net;
using System.Net.Http.Headers;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace Shiny.Actors.Remoting;


/// <summary>
/// Calls actors exposed by Shiny.Actors.HttpServer. <see cref="HttpClient.BaseAddress"/> is the server's actor
/// prefix, e.g. <c>http://192.168.1.20:8080/actors/</c>. Authentication is whatever the HttpClient sends.
/// </summary>
public sealed class HttpActorTransport(HttpClient httpClient) : IActorTransport
{
    static readonly MediaTypeHeaderValue Json = new("application/json");


    public async ValueTask<byte[]?> InvokeAsync(ActorContract contract, string actorId, ActorMethod method, byte[] arguments, IReadOnlyDictionary<string, string> requestContext, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, this.Url("call", contract.Name, actorId, method.Key))
        {
            Content = new ByteArrayContent(arguments) { Headers = { ContentType = Json } }
        };
        // traceparent is added by HttpClient itself from Activity.Current
        if (ActorWire.EncodeRequestContext(requestContext) is { } context)
            request.Headers.TryAddWithoutValidation(ActorWire.RequestContextHeader, context);
        using var response = await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await ThrowIfFailedAsync(response, cancellationToken).ConfigureAwait(false);

        return response.StatusCode is HttpStatusCode.NoContent or HttpStatusCode.Accepted
            ? null
            : await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
    }


    public async ValueTask PublishAsync(string streamName, string key, byte[] item, CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, this.Url("streams", streamName, key))
        {
            Content = new ByteArrayContent(item) { Headers = { ContentType = Json } }
        };
        using var response = await httpClient.SendAsync(request, cancellationToken).ConfigureAwait(false);
        await ThrowIfFailedAsync(response, cancellationToken).ConfigureAwait(false);
    }


    public async IAsyncEnumerable<RemoteStreamEvent> SubscribeAsync(string streamName, string key, long? afterSequence, bool requireDurable, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var url = this.Url("streams", streamName, key) + (requireDurable ? "?durable=1" : "");
        using var request = new HttpRequestMessage(HttpMethod.Get, url);
        request.Headers.Accept.Add(new MediaTypeWithQualityHeaderValue("text/event-stream"));
        if (afterSequence is { } after)
            request.Headers.TryAddWithoutValidation("Last-Event-ID", after.ToString(System.Globalization.CultureInfo.InvariantCulture));

        using var response = await httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken).ConfigureAwait(false);
        await ThrowIfFailedAsync(response, cancellationToken).ConfigureAwait(false);

        await using var body = await response.Content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        using var reader = new StreamReader(body, Encoding.UTF8);
        var data = new StringBuilder();
        long sequence = 0;

        // server-sent events: "data:" lines accumulate until a blank line ends the event; "id:" is the sequence; ":" lines are comments
        while (await reader.ReadLineAsync(cancellationToken).ConfigureAwait(false) is { } line)
        {
            if (line.Length == 0)
            {
                if (data.Length > 0)
                {
                    yield return new RemoteStreamEvent(sequence, Encoding.UTF8.GetBytes(data.ToString()));
                    data.Clear();
                    sequence = 0;
                }
            }
            else if (line.StartsWith("data:", StringComparison.Ordinal))
            {
                if (data.Length > 0)
                    data.Append('\n');
                data.Append(line.AsSpan(line.StartsWith("data: ", StringComparison.Ordinal) ? 6 : 5));
            }
            else if (line.StartsWith("id:", StringComparison.Ordinal))
            {
                long.TryParse(line.AsSpan(3).Trim(), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out sequence);
            }
        }
    }


    string Url(params string[] segments)
    {
        var root = httpClient.BaseAddress?.ToString() ?? throw new InvalidOperationException(
            "Set HttpClient.BaseAddress to the server's actor prefix, e.g. http://device:8080/actors/"
        );
        return root.TrimEnd('/') + "/" + string.Join("/", segments.Select(ActorWire.EncodeSegment));
    }


    static async ValueTask ThrowIfFailedAsync(HttpResponseMessage response, CancellationToken cancellationToken)
    {
        if (response.IsSuccessStatusCode)
            return;

        RemoteError? error = null;
        try
        {
            var bytes = await response.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
            if (bytes.Length > 0 && response.Content.Headers.ContentType?.MediaType == "application/json")
                error = JsonSerializer.Deserialize(bytes, RemotingJsonContext.Default.RemoteError);
        }
        catch (JsonException) { }

        throw new RemoteActorException(
            (int)response.StatusCode,
            error?.Error ?? response.StatusCode.ToString(),
            error?.Message ?? $"The actor server answered {(int)response.StatusCode} {response.ReasonPhrase}."
        );
    }
}
