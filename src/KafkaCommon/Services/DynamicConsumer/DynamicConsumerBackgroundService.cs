namespace KafkaCommon.Services.DynamicConsumer;

using Confluent.Kafka;
using Abstractions;
using ClientBuilders;
using Configuration;
using Interfaces;
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
    }

    protected CancellationToken StoppingToken
        => _cts.Token;

    protected IConsumer<TKey, TValue> Consumer { get; set; }
    protected TopicConfiguration ConsumerConfiguration { get; set; }
    protected IConsumer<TKey, byte[]> ByteConsumer { get; set; }

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
            _logger.LogError($"An error occured during {nameof(IsConsumerSet)}", e);
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
            {
                configuration.Settings["group.id"] = Constants.GroupIdGuidPlaceholder;
            }
            Consumer = StaticConsumerBuilder.BuildConsumer(configuration, keyDeserializer, valueDeserializer, _eventsHandler);
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

    private bool IsGuidPlaceholderMissingFromGroupId(TopicConfiguration configuration)
        => configuration.Settings.ContainsKey("group.id") &&
           !configuration.Settings["group.id"].Contains(Constants.GroupIdGuidPlaceholder);
}