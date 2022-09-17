namespace DynamicKafkaConsumer;

using System.Text.Json;
using System.Text.Json.Serialization;

public static class SerializerOptions
{
    public static JsonSerializerOptions ConfigureSerializerOptions()
    {
        var options = new JsonSerializerOptions
        {
            NumberHandling = JsonNumberHandling.AllowReadingFromString | JsonNumberHandling.AllowNamedFloatingPointLiterals
        };
        options.Converters.Add(new JsonStringEnumConverter());
        //options.Converters.Add(new JsonStringEnumMemberConverter());
        return options;
    }
}