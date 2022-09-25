namespace KafkaCommon.Services.Producers;

using Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Options;

public class BasicProducer<TKey, TValue> : IDisposable
    where TValue : class
{
    private const string CompressionTypeSetting = "compression.type";
    private const int DefaultFlushTimeoutInMilliseconds = 500;
    private const int FullQueueFlushTimeoutInMilliseconds = 10_000;
    private const int ShutDownFlushTimeoutInMilliseconds = 30_000;
    
    private readonly IOptionsMonitor<KafkaConfiguration> _kafkaConfiguration;
    private readonly IProducer<TKey, TValue> _producer;

    public BasicProducer(IOptionsMonitor<KafkaConfiguration> kafkaConfiguration, ISerializer<TValue> serializer)
    {
        _kafkaConfiguration = kafkaConfiguration;
        ProducerConfig = _kafkaConfiguration.CurrentValue.Producers[ConfigurationSectionName];

        if (!ProducerConfig.Topics.Any() && _kafkaConfiguration.CurrentValue.BaseConfig.Topics.Any())
            ProducerConfig.Topics = _kafkaConfiguration.CurrentValue.BaseConfig.Topics;

        if (ProducerConfig.Settings.ContainsKey(CompressionTypeSetting))
            ProducerConfig.Settings.Add(CompressionTypeSetting, CompressionType.Gzip.ToString());

        _producer = new ProducerBuilder<TKey, TValue>(ProducerConfig.Settings)
            .SetValueSerializer(serializer)
            .Build();
    }

    protected virtual string ConfigurationSectionName => nameof(BasicProducer<TKey, TValue>);
    
    protected TopicConfiguration ProducerConfig { get; }

    public void Produce(TKey key, TValue item)
        => Produce(new Message<TKey, TValue> { Key = key, Value = item });
    
    public void Produce(TKey key, TValue item, Headers headers)
        => Produce(new Message<TKey, TValue> { Key = key, Value = item, Headers =  headers});

    public void Produce(Message<TKey, TValue> message)
        => Produce(message, null);

    protected virtual void Produce(Message<TKey, TValue> message, Partition? partition)
    {
        const int maxRetriesOnQueueFull = 5;

        int retry = 0;
        while (++retry <= maxRetriesOnQueueFull)
        {
            try
            {
                if (partition == null)
                {
                    foreach (var topic in ProducerConfig.Topics)
                    {
                        _producer.Produce(topic, message);
                    }
                }
                else
                {
                    foreach (var topic in ProducerConfig.Topics)
                    {
                        _producer.Produce(new TopicPartition(topic, partition.Value), message);
                    }
                }
                return;
            }
            catch (ProduceException<TKey, TValue> e)
            {
                if (e.Error.Code == ErrorCode.Local_QueueFull)
                {
                    Flush(FullQueueFlushTimeoutInMilliseconds);
                }
                else
                {
                    throw;
                }
            }
        }
    }

    public int Flush(int flushTimeoutInMilliseconds = DefaultFlushTimeoutInMilliseconds)
        => _producer.Flush(TimeSpan.FromMilliseconds(flushTimeoutInMilliseconds));

    public virtual void Dispose()
    {
        Flush(ShutDownFlushTimeoutInMilliseconds);
        _producer.Dispose();
    }
}