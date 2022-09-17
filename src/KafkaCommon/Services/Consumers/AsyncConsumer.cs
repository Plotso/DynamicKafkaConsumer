namespace KafkaCommon.Services.Consumers;

using Interfaces;
using KafkaCommon.ClientBuilders;
using KafkaCommon.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Fully functional consumer service.
/// Ready to use class, just make a consumer wrapper that would inherit the class (the idea is that many consumer with different names can be registered)
/// </summary>
public class AsyncConsumer<TKey, TValue> : ConsumerWorker<TKey, TValue>
{
    //ToDo: Those 2 below should be extracted to configurations
    private const int ConsumerPoolingTimeoutInMs = 10_000;
    private const int MaxRetriesForProcessing = 3;

    public AsyncConsumer(
        ConsumerBuilderTopic<TKey, TValue> builder,
        IEnumerable<IMessageProcessor<TKey, TValue>> messageProcessors,
        IOptionsMonitor<KafkaConfiguration> kafkaConfiguration,
        ILogger logger) 
        : base(messageProcessors, builder, kafkaConfiguration, logger)
    { }

    public override async Task StartAsync(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                Subscribe(cancellationToken);
                await StartConsumer(cancellationToken);
            }
            catch (Exception e)
            {
                Logger.LogError(e, "Unhandled exception occured during consumer service operation");
            }
            finally
            {
                CommitOffset();
                Unsubscribe(cancellationToken);
            }
        }
    }

    private async Task StartConsumer(CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                var processorReadyStateTasks = MessageProcessors
                    .Select(p => WaitProcessorToBeInReadyState(p, cancellationToken))
                    .ToList();
                await Task.WhenAll(processorReadyStateTasks).ConfigureAwait(false);

                var message = Consumer.Consume(TimeSpan.FromMilliseconds(ConsumerPoolingTimeoutInMs));

                if (message.IsPartitionEOF)
                {
                    Logger.LogDebug($"Reached partition EOF for partition {message.Partition.Value}. Offset {message.Offset.Value}");
                    continue;
                }

                var processors = MessageProcessors
                    .Where(p => p.ShouldProcessMessage(message, cancellationToken).Result);
                if (processors.Any())
                {
                    foreach (var processor in processors)
                    {
                        var isSuccess = false;
                        var attemptCounter = 1;
                        while (!isSuccess && attemptCounter <= MaxRetriesForProcessing && !cancellationToken.IsCancellationRequested)
                        {
                            attemptCounter++;
                            isSuccess = await processor.TryProcessMessage(message, cancellationToken);
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
}