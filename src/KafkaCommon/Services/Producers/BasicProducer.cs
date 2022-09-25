namespace KafkaCommon.Services.Producers;

using Configuration;
using Confluent.Kafka;
using Extensions;
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

        if (ProducerConfig.Topics.IsNullOrEmpty() && _kafkaConfiguration.CurrentValue.BaseConfig.Topics.IsNullOrEmpty())
            throw new ArgumentNullException($"No topics provided neither in base nor topic configurations for following producer configuration: {ConfigurationSectionName}");
        
        if (ProducerConfig.Topics.IsNullOrEmpty() && _kafkaConfiguration.CurrentValue.BaseConfig.Topics.Any())
            ProducerConfig.Topics = _kafkaConfiguration.CurrentValue.BaseConfig.Topics;

        MergeKafkaSettings();
        if (!ProducerConfig.Settings.ContainsKey(CompressionTypeSetting))
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
    
    private void MergeKafkaSettings()
    {
        var baseSettings = _kafkaConfiguration.CurrentValue.BaseConfig?.BaseSettings;
        if (baseSettings != null && baseSettings.Any())
        {
            if (ProducerConfig.Settings == null)
                ProducerConfig.Settings = new Dictionary<string, string>();
            
            foreach (var setting in baseSettings)
            {
                if (!ProducerConfig.Settings.ContainsKey(setting.Key))
                    ProducerConfig.Settings.Add(setting.Key, setting.Value);
            }
        }
    }
}