namespace KafkaCommon.Serialization;

using System.Text.Json;
using Confluent.Kafka;

public static class DeserializerInstances
{
    public class JsonDeserializer<T> : IDeserializer<T>
    {
        public T Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => JsonSerializer.Deserialize<T>(data) ?? default;
    }

    public class NullDeserializer : IDeserializer<Null>
    {
        public Null Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Null.Deserialize(data, isNull, context);
    }
    public class IgnoreDeserializer : IDeserializer<Ignore>
    {
        public Ignore Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Ignore.Deserialize(data, isNull, context);
    }
    public class Utf8Deserializer : IDeserializer<string>
    {
        public string Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Utf8.Deserialize(data, isNull, context);
    }
    public class IntDeserializer : IDeserializer<int>
    {
        public int Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Int32.Deserialize(data, isNull, context);
    }
    public class LongDeserializer : IDeserializer<long>
    {
        public long Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Int64.Deserialize(data, isNull, context);
    }
    public class DoubleDeserializer : IDeserializer<double>
    {
        public double Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Double.Deserialize(data, isNull, context);
    }
    public class FloatDeserializer : IDeserializer<float>
    {
        public float Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.Single.Deserialize(data, isNull, context);
    }
    public class ByteArrayDeserializer : IDeserializer<byte[]>
    {
        public byte[] Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context) 
            => Deserializers.ByteArray.Deserialize(data, isNull, context);
    }
    
    public static IDeserializer<T> GetDeserializer<T>()
    {
        if (typeof(T) == typeof(Ignore))
            return (IDeserializer<T>)Deserializers.Ignore;

        if (typeof(T) == typeof(Null))
            return (IDeserializer<T>)Deserializers.Null;

        if (typeof(T) == typeof(byte[]))
            return (IDeserializer<T>)Deserializers.ByteArray;

        if (typeof(T) == typeof(int))
            return (IDeserializer<T>)Deserializers.Int32;

        if (typeof(T) == typeof(long))
            return (IDeserializer<T>)Deserializers.Int64;

        if (typeof(T) == typeof(double))
            return (IDeserializer<T>)Deserializers.Double;

        if (typeof(T) == typeof(float))
            return (IDeserializer<T>)Deserializers.Single;

        if (typeof(T) == typeof(string))
            return (IDeserializer<T>)Deserializers.Utf8;

        if (typeof(T) == typeof(byte[]))
            return (IDeserializer<T>)Deserializers.ByteArray;

        throw new ArgumentException($"Deserializer of type {typeof(T)} is not supported");
    }
}