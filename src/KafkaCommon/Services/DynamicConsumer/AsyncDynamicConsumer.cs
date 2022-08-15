namespace KafkaCommon.Services.DynamicConsumer;

using Confluent.Kafka;
using Interfaces;
using Abstractions;
using Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// When triggered, process all present messages on for specific keys. Then waits for new trigger.
/// Ready to use class, just make a consumer wrapper that would inherit the class (the idea is that many consumer with different names can be registered)
/// </summary>
public abstract class AsyncDynamicConsumer<TKey, TValue> : DynamicConsumerWorker<TKey, TValue>
{
    private readonly IDynamicConsumerModifier<TKey> _dynamicConsumerModifier;
    private HashSet<TKey> _completelyProcessedKeys = new();

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
                if (_dynamicConsumerModifier.GetKeysToProcess().Any())
                {
                    Subscribe(cancellationToken);
                    isSubscribed = true;
                    await StartConsumer(cancellationToken);
                }
                else
                {
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
                }
            }
        }
    }

    private async Task StartConsumer(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var keysToProcess = _dynamicConsumerModifier.GetKeysToProcess();
                // Wait until keys are configured through IDynamicConsumerModifier
                if (!keysToProcess.Any())
                {
                    await Task.Delay(CheckingForKeysConfigurationDelay);
                    continue;
                }

                if (keysToProcess.Any() && !IsConsumerSet())
                {
                    BuildDynamicConsumer(ConsumerConfigurationName(), KeyDeserializer, ValueDeserializer);
                }
                if (keysToProcess.All(key => _completelyProcessedKeys.Contains(key)))
                {
                    _dynamicConsumerModifier.MarkKeysAsProcessed();
                    _completelyProcessedKeys = new HashSet<TKey>();
                    DisposeCurrentConsumer();
                }
                
                var processorReadyStateTasks = MessageProcessors
                    .Select(p => WaitProcessorToBeInReadyState(p, cancellationToken))
                    .ToList();
                await Task.WhenAll(processorReadyStateTasks).ConfigureAwait(false);

                var consumerResult = Consumer.Consume(TimeSpan.FromMilliseconds(ConsumerPoolingTimeoutInMs));

                if (!keysToProcess.Contains(consumerResult.Message.Key))
                    continue;

                if (consumerResult.IsPartitionEOF)
                {
                    _completelyProcessedKeys.Add(consumerResult.Message.Key);
                    Logger.LogDebug($"Reached partition EOF for partition {consumerResult.Partition.Value}. Offset {consumerResult.Offset.Value}");
                    continue;
                }

                var processors = MessageProcessors
                    .Where(p => p.ShouldProcessMessage(consumerResult, cancellationToken).Result);
                if (processors.Any())
                {
                    foreach (var processor in processors)
                    {
                        var isSuccess = false;
                        var attemptCounter = 1;
                        while (!isSuccess && attemptCounter <= MaxRetriesForProcessing && !cancellationToken.IsCancellationRequested)
                        {
                            attemptCounter++;
                            isSuccess = await processor.TryProcessMessage(consumerResult, cancellationToken);
                        }
                    }
                }
                
                //ToDo: Add custom logic for commiting offsets
            }
            catch (Exception e)
            {
                Logger.LogError(e, "An error occured during consumer work");
            }
        }
    }
}