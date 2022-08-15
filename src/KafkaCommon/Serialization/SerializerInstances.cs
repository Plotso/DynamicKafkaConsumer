namespace KafkaCommon.Serialization;

using System.Text.Json;
using Confluent.Kafka;

public static class SerializerInstances
{
    public class JsonValueSerializer<T> : ISerializer<T>
    {
        public byte[] Serialize(T data, SerializationContext context) => JsonSerializer.SerializeToUtf8Bytes(data);
    }
}