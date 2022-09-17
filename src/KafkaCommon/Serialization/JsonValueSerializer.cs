namespace KafkaCommon.Serialization;

using System.Text.Json;
using System.Text.Json.Serialization;
using Confluent.Kafka;

public class JsonValueSerializer<TValue> : IDeserializer<TValue>, ISerializer<TValue> where TValue : class
{
    private readonly JsonSerializerOptions _serializerOptions;

    public JsonValueSerializer(JsonSerializerOptions jsonSerializerOptions = null)
    {
        _serializerOptions = jsonSerializerOptions ?? new JsonSerializerOptions();
    }

    public byte[] Serialize(TValue data, SerializationContext context)
        => data == null
            ? null
            : JsonSerializer.SerializeToUtf8Bytes(data, _serializerOptions);

    public TValue Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        => isNull ? null : JsonSerializer.Deserialize<TValue>(data, _serializerOptions);
}