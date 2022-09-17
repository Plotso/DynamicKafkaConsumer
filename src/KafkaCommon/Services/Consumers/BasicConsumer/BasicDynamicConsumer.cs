namespace KafkaCommon.Services.Consumers.BasicConsumer;

using Confluent.Kafka;
using Interfaces;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using KafkaCommon.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Dynamic Consumer that starts on demand when the applications configures specific keys to be processed (via implementation of <see cref="IDynamicConsumerModifier{TConsumerKey}"/>).
/// Once the provided keys are fully processed, the internal consumer is disposed and on new request, new one is being made
/// </summary>
public class BasicDynamicConsumer<TKey, TValue> : BaseBasicConsumer<TKey, TValue>
    where TValue : class
{
    private readonly IDynamicConsumerModifier<TKey> _dynamicConsumerModifier;
    private readonly ILogger<BasicDynamicConsumer<TKey, TValue>> _logger;
    private HashSet<int> _assignedPartitions = new();
    private HashSet<int> _processedPartitions = new();

    public BasicDynamicConsumer(
        IOptionsMonitor<KafkaConfiguration> config,
        IConsumerEventsHandler consumerEventsHandler,
        IDynamicConsumerModifier<TKey> dynamicConsumerModifier,
        JsonValueSerializer<TValue> serializer,
        ILogger<BasicDynamicConsumer<TKey, TValue>> logger) : base(config, consumerEventsHandler, logger, serializer)
    {
        _dynamicConsumerModifier = dynamicConsumerModifier;
        _logger = logger;
    }

    //Protected so that it can be overriden if used with multiple consumers
    protected virtual int CheckingForKeysConfigurationDelay => 30;
    protected override string ConsumerConfigurationName => nameof(BasicDynamicConsumer<TKey, TValue>);

    public async Task StartAsync(Func<Message<TKey, TValue>, Task> messageHandler, CancellationToken cancellationToken)
    {
        var isSubscribed = false;
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                if (_dynamicConsumerModifier.GetKeysToProcess()?.Any() ?? false)
                {
                    if (!IsConsumerSet())
                    {
                        BuildConsumer();
                    }

                    Subscribe(cancellationToken);
                    isSubscribed = true;

                    while (!ShouldStartConsuming() && !cancellationToken.IsCancellationRequested)
                    {
                        _logger.LogInformation(
                            $"Waiting for application to become ready before starting to consume from topics: {string.Join(" ,", ConsumerConfig.Topics)}");
                        await Task.Delay(1000, cancellationToken);
                    }

                    await StartConsumer(cancellationToken, messageHandler);
                }
                else
                {
                    await Task.Delay(CheckingForKeysConfigurationDelay);
                }
            }
            catch (Exception e)
            {
                _logger.LogError(e, "Unhandled exception occured during consumer service operation");
            }
            finally
            {
                if (isSubscribed)
                {
                    Unsubscribe(cancellationToken);
                    isSubscribed = false;
                }
            }
        }
    }

    /// <summary>
    /// 
    /// </summary>
    /// <param name="cancellationToken"></param>
    /// <param name="messageHandler"></param>
    private async Task StartConsumer(CancellationToken cancellationToken,
        Func<Message<TKey, TValue>, Task> messageHandler)
    {
        try
        {
            var isDone = false;
            var consumedAnything = false;
            var markedAllNonRelevantPartitionsAsProcessed = false;
            var keysToProcess = _dynamicConsumerModifier.GetKeysToProcess()?.ToList();
            try
            {
                while (!cancellationToken.IsCancellationRequested &&
                       !isDone)
                {
                    // End consumer if we've reached PartitionEOF for every configured key
                    if (consumedAnything && _assignedPartitions.All(p => _processedPartitions.Contains(p)))
                    {
                        isDone = true;
                        continue;
                    }

                    var consumeResult = Consumer.Consume(cancellationToken);
                    _assignedPartitions = Consumer.Assignment.Select(p => p.Partition.Value).ToHashSet();
                    consumedAnything = true;

                    if (consumeResult.IsPartitionEOF)
                    {
                        _logger.LogDebug(
                            $"Reached partition EOF for partition {consumeResult.Partition.Value}. Offset {consumeResult.Offset.Value}");
                        _processedPartitions.Add(consumeResult.Partition.Value);
                        continue;
                    }

                    if (consumeResult == null ||
                        !keysToProcess.Contains(consumeResult.Message.Key))
                        continue;

                    //If a single key is configured to be processed, kafka ensures that it will be in the same partition, so we internally mark all others partitions as processed to save time
                    // If case above ensures current message is for the configured key so that we can take accurate partition
                    if (keysToProcess.Count == 1 && !markedAllNonRelevantPartitionsAsProcessed)
                    {
                        var currentPartition = consumeResult.Partition.Value;
                        var partitionsToMarkAsCompleted = _assignedPartitions.Where(p => p != currentPartition);
                        foreach (var partition in partitionsToMarkAsCompleted)
                        {
                            _processedPartitions.Add(partition);
                        }

                        markedAllNonRelevantPartitionsAsProcessed = true;
                    }

                    if (!ShouldProcessMessage(consumeResult))
                        continue;

                    // Pass message rather than value so that the messageHandler has access to Headers
                    await messageHandler(consumeResult.Message);

                    HandleNotCommittedOffsets();
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"An error ocurred during dynamic consumer processing for keys: {string.Join(" ,", keysToProcess)}");
            }
        }
        finally
        {
            _processedPartitions = new HashSet<int>();
            _assignedPartitions = new HashSet<int>();
            _dynamicConsumerModifier.MarkKeysAsProcessed();
            SafeCommitOffset();
            DisposeCurrentConsumer();
            _logger.LogInformation($"DynamicConsumer {ConsumerConfigurationName} disposed");
        }
    }
}