namespace KafkaCommon.Services.Consumers.DynamicConsumer;

using Confluent.Kafka;
using Interfaces;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// When triggered, process all present messages on for specific keys. Then waits for new trigger.
/// Ready to use class, just make a consumer wrapper that would inherit the class (the idea is that many consumer with different names can be registered)
/// </summary>
public abstract class AsyncDynamicConsumer<TKey, TValue> : DynamicConsumerWorker<TKey, TValue>
{
    private readonly IDynamicConsumerModifier<TKey> _dynamicConsumerModifier;
    private HashSet<int> _assignedPartitions = new();
    private HashSet<int> _completedPartitions = new();

    private const int ConsumerPoolingTimeoutInMs = 10_000;
    private const int MaxRetriesForProcessing = 3;

    public AsyncDynamicConsumer(
        IDynamicConsumerModifier<TKey> dynamicConsumerModifier,
        IOptionsMonitor<KafkaConfiguration> kafkaConfiguration,
        IConsumerEventsHandler? eventsHandler,
        IEnumerable<IMessageProcessor<TKey, TValue>> messageProcessors,
        ILogger logger)
        : base(messageProcessors, kafkaConfiguration, eventsHandler, logger)
    {
        _dynamicConsumerModifier = dynamicConsumerModifier;
    }

    protected abstract string ConsumerConfigurationName();

    protected IDeserializer<TKey>? KeyDeserializer => null;
    protected IDeserializer<TValue>? ValueDeserializer => null;

    protected const int CheckingForKeysConfigurationDelay = 30;

    public override async Task StartAsync(CancellationToken cancellationToken)
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
                        BuildDynamicConsumer(ConsumerConfigurationName(), KeyDeserializer, ValueDeserializer);
                    }

                    Subscribe(cancellationToken);
                    isSubscribed = true;
                    await StartConsumer(cancellationToken);
                }
                else
                {
                    // Wait until keys are configured through IDynamicConsumerModifier
                    await Task.Delay(CheckingForKeysConfigurationDelay);
                }
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unhandled exception occured during consumer service operation");
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

    private async Task StartConsumer(CancellationToken cancellationToken)
    {
        try
        {
            var isDone = false;
            var consumedAnything = false;
            var markedAllNonRelevantPartitionsAsProcessed = false;
            var keysToProcess = _dynamicConsumerModifier.GetKeysToProcess().ToList();
            while (!cancellationToken.IsCancellationRequested && !isDone)
            {
                try
                {
                    // End consumer if we've reached PartitionEOF for every configured key
                    if (consumedAnything && _completedPartitions.All(p => _assignedPartitions.Contains(p)))
                    {
                        isDone = true;
                        continue;
                    }

                    var processorReadyStateTasks = MessageProcessors
                        .Select(p => WaitProcessorToBeInReadyState(p, cancellationToken))
                        .ToList();
                    await Task.WhenAll(processorReadyStateTasks).ConfigureAwait(false);

                    var consumeResult = Consumer.Consume(TimeSpan.FromMilliseconds(ConsumerPoolingTimeoutInMs));
                    _assignedPartitions = Consumer.Assignment.Select(a => a.Partition.Value).ToHashSet();
                    consumedAnything = true;

                    if (consumeResult.IsPartitionEOF)
                    {
                        _completedPartitions.Add(consumeResult.Partition.Value);
                        Logger.LogDebug($"Reached partition EOF for partition {consumeResult.Partition.Value}. Offset {consumeResult.Offset.Value}");
                        continue;
                    }

                    if (!keysToProcess.Contains(consumeResult.Message.Key))
                        continue;

                    // If a single key is configured, kafka ensures that all messages with that key will be in same partition. This means others are non-relevant for our processing.
                    if (keysToProcess.Count == 1 && !markedAllNonRelevantPartitionsAsProcessed)
                    {
                        MarkNonRelevantPartitionsAsProcessed(keysToProcess, consumeResult);
                        markedAllNonRelevantPartitionsAsProcessed = true;
                    }

                    var processors = MessageProcessors
                        .Where(p => p.ShouldProcessMessage(consumeResult, cancellationToken).Result);
                    if (processors.Any())
                    {
                        foreach (var processor in processors)
                        {
                            var isSuccess = false;
                            var attemptCounter = 1;
                            while (!isSuccess && attemptCounter <= MaxRetriesForProcessing &&
                                   !cancellationToken.IsCancellationRequested)
                            {
                                attemptCounter++;
                                isSuccess = await processor.TryProcessMessage(consumeResult, cancellationToken);
                            }
                        }
                    }

                    HandleNotCommittedOffsets();
                }
                catch (Exception e)
                {
                    Logger.LogError(e, "An error occured during consumer work");
                }
            }
        }
        finally
        {
            _dynamicConsumerModifier.MarkKeysAsProcessed();
            _completedPartitions = new HashSet<int>();
            SafeCommitOffset();
            DisposeCurrentConsumer();
            Logger.LogInformation($"DynamicConsumer disposed");
        }
    }

    private void MarkNonRelevantPartitionsAsProcessed(List<TKey> keysToProcess,
        ConsumeResult<TKey, TValue> consumeResult)
    {
        //If a single key is configured to be processed, kafka ensures that it will be in the same partition, so we internally mark all others partitions as processed to save time
        // If case above ensures current message is for the configured key so that we can take accurate partition
        if (keysToProcess.Count == 1)
        {
            var currentPartition = consumeResult.Partition.Value;
            var partitionsToMarkAsCompleted = _assignedPartitions.Where(p => p != currentPartition);
            foreach (var partition in partitionsToMarkAsCompleted)
            {
                _completedPartitions.Add(partition);
            }
        }
    }
}