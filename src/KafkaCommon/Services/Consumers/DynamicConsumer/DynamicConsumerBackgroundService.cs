namespace KafkaCommon.Services.Consumers.DynamicConsumer;

using Confluent.Kafka;
using Interfaces;
using KafkaCommon.Abstractions;
using KafkaCommon.ClientBuilders;
using KafkaCommon.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Contains base logic required to use when starting a dynamic consumer.
/// </summary>
public abstract class DynamicConsumerBackgroundService<TKey, TValue> : IDynamicConsumerService
{
    private readonly IOptionsMonitor<KafkaConfiguration> _kafkaConfiguration;
    private readonly IConsumerEventsHandler? _eventsHandler;
    private readonly ILogger _logger;
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public DynamicConsumerBackgroundService(IOptionsMonitor<KafkaConfiguration> kafkaConfiguration, IConsumerEventsHandler? eventsHandler, ILogger logger)
    {
        _kafkaConfiguration = kafkaConfiguration;
        _eventsHandler = eventsHandler;
        _logger = logger;
        
        NotCommittedMessagesCount = 0;
    }

    protected CancellationToken StoppingToken
        => _cts.Token;

    protected IConsumer<TKey, TValue> Consumer { get; set; }
    protected TopicConfiguration ConsumerConfiguration { get; set; }
    protected IConsumer<TKey, byte[]> ByteConsumer { get; set; }
        
    protected int NotCommittedMessagesCount { get; set; }

    protected virtual bool ManuallyHandleOffsetCommit => false;
    public List<TopicPartition> PartitionsAssigned() => Consumer.Assignment;

    public abstract Task StartAsync(CancellationToken cancellationToken);

    public virtual Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        try 
        {
            _cts.Cancel();
            Consumer?.Close();
            Consumer?.Dispose();
        } catch (ObjectDisposedException)  { /* suppress */ }
    }
    
    public  bool IsConsumerSet()
    {
        try
        {
            var _ = Consumer.Assignment;
            return true;
        }
        catch (ObjectDisposedException e)
        {
            return false;
        }
        catch (Exception e)
        {
            _logger.LogError(e, $"An error occured during {nameof(IsConsumerSet)}");
            return false;
        }
    }

    /// <summary>
    /// Builds a consumer with current configuration.Overrides consumer group in case of missing consumerGroup
    /// </summary>
    /// <param name="consumerConfigurationName"></param>
    /// <param name="keyDeserializer"></param>
    /// <param name="valueDeserializer"></param>
    protected void BuildDynamicConsumer(string consumerConfigurationName,
        IDeserializer<TKey>? keyDeserializer = null,
        IDeserializer<TValue>? valueDeserializer = null)
    {
        if (_kafkaConfiguration.CurrentValue.Consumers.TryGetValue(consumerConfigurationName, out var configuration))
        {
            if (IsGuidPlaceholderMissingFromGroupId(configuration))
                configuration.Settings["group.id"] = Constants.GroupIdGuidPlaceholder;

            if (!configuration.Topics.Any() && _kafkaConfiguration.CurrentValue.BaseSettings.Topics.Any())
                configuration.Topics = _kafkaConfiguration.CurrentValue.BaseSettings.Topics;
            
            Consumer = StaticConsumerBuilder.BuildConsumer(configuration, keyDeserializer, valueDeserializer, _eventsHandler, consumerConfigurationName);
            ConsumerConfiguration = configuration;
        }
    }

    protected void DisposeCurrentConsumer()
    {
        try 
        {
            Consumer?.Close();
            Consumer?.Dispose();
        } catch (ObjectDisposedException)  { /* suppress */ }
    }

    public void ManuallyCommitOffset(ConsumeResult<TKey, TValue> consumeResult) => Consumer.Commit(consumeResult);

    public void ManuallyCommitOffset(IEnumerable<TopicPartitionOffset> offsets) => Consumer.Commit(offsets);
    
    // Executes CommitOffset but suppresses any potential exception
    protected void SafeCommitOffset()
    {
        try
        {
            CommitOffset();
        }
        catch (Exception){ /* suppress */ }
    }

    protected void CommitOffset()
    {
        if (ManuallyHandleOffsetCommit)
            return;
            
        if (NotCommittedMessagesCount > 0)
            Consumer.Commit();
    }

    protected void HandleNotCommittedOffsets()
    {
        NotCommittedMessagesCount++;
        if (ManuallyHandleOffsetCommit)
            return;
            
        if (NotCommittedMessagesCount >= ConsumerConfiguration.MaxNotCommittedMessages)
        {
            Consumer.Commit();
            NotCommittedMessagesCount = 0;
        }
    }

    private bool IsGuidPlaceholderMissingFromGroupId(TopicConfiguration configuration)
        => configuration.Settings.ContainsKey("group.id") &&
           !configuration.Settings["group.id"].Contains(Constants.GroupIdGuidPlaceholder);
}