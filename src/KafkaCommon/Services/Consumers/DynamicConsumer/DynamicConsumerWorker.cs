namespace KafkaCommon.Services.Consumers.DynamicConsumer;

using Interfaces;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

/// <summary>
/// Contains base logic required for a consumer to subscribe to topics and to work with collection of <see cref="IMessageProcessor{TKey,TMessage}"/>
///  It's a standalone class since the base class <see cref="DynamicConsumerBackgroundService{TKey,TValue}"/> might be used in a place where it won't depend
/// on IMessageProcessors so new ConsumerWorker would be required
/// </summary>
public abstract class DynamicConsumerWorker<TKey, TValue> : DynamicConsumerBackgroundService<TKey, TValue>, IPartitionConsumer<TKey, TValue>
{
    public DynamicConsumerWorker(
        IEnumerable<IMessageProcessor<TKey, TValue>> messageProcessors,
        IOptionsMonitor<KafkaConfiguration> kafkaConfiguration,
        IConsumerEventsHandler? eventsHandler,
        ILogger logger) 
        : base(kafkaConfiguration, eventsHandler, logger)
    {
        MessageProcessors = messageProcessors;
        Logger = logger;
    }
    protected IEnumerable<IMessageProcessor<TKey, TValue>> MessageProcessors { get; }
    protected ILogger Logger { get; }

    protected async Task WaitProcessorToBeInReadyState(IMessageProcessor<TKey, TValue> processor, CancellationToken cancellation)
    {
        bool readyForProcessing;
        do
        {
            try
            {
                readyForProcessing = await processor.ReadyForProcessingMessages(cancellation);
                if (!readyForProcessing)
                {
                    Logger.LogInformation("Processor is not in ready state");
                    await Task.Delay(TimeSpan.FromSeconds(1), cancellation);
                }
            }
            catch (Exception e)
            {
                readyForProcessing = false;
                Logger.LogError(e, "An error occurred while waiting for processor to become ready");
                await Task.Delay(TimeSpan.FromSeconds(5), cancellation);
            }

        } while (!readyForProcessing && !cancellation.IsCancellationRequested);
    }
    
    public void Subscribe(CancellationToken cancellationToken)
    {
        var isSubscribed = false;
        do
        {
            try
            {
                Consumer.Subscribe(ConsumerConfiguration.Topics);
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to subscribe to topics: {ConsumerConfiguration.Topics}");
            }
        } while (!isSubscribed && !cancellationToken.IsCancellationRequested);
    }

    public void Unsubscribe(CancellationToken cancellationToken)
    {
        const int secondsToUnsubscribe = 120;
        var unsubscribeUntil = DateTime.Now.AddSeconds(secondsToUnsubscribe);
        var isSuccess = false;
        do
        {
            try
            {
                Consumer.Unsubscribe();
                isSuccess = true;
            }
            catch (ObjectDisposedException e)
            {
                isSuccess = true;
            }
            catch (Exception e)
            {
                Logger.LogError(e, $"Failed to unsubscribe from topics: {ConsumerConfiguration.Topics}");
            }

            Thread.Sleep(25);
        } while (!isSuccess && !cancellationToken.IsCancellationRequested && unsubscribeUntil >= DateTime.Now);
    }
}