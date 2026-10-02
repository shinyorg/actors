using System.Text.Json.Serialization;

namespace Shiny.Actors;


[JsonSourceGenerationOptions(PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(List<ActorReminder>))]
partial class ActorsJsonContext : JsonSerializerContext;
