namespace KafkaCommon.Services.Consumers.BasicConsumer;

using Confluent.Kafka;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using KafkaCommon.Serialization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

public class BasicConsumer<TKey, TValue> : BaseBasicConsumer<TKey, TValue>
    where TValue : class
{
    private readonly ILogger<BasicConsumer<TKey, TValue>> _logger;

    public BasicConsumer(
        IOptionsMonitor<KafkaConfiguration> config,
        IConsumerEventsHandler consumerEventsHandler,
        JsonValueSerializer<TValue> serializer,
        ILogger<BasicConsumer<TKey, TValue>> logger) 
        : base(config, consumerEventsHandler, logger, serializer)
    {
        _logger = logger;
    }
        
    //Protected so that it can be overriden if used with multiple consumers
    protected override string ConsumerConfigurationName => nameof(BasicConsumer<TKey, TValue>);

    public async Task StartAsync(Func<Message<TKey, TValue>, Task> messageHandler, CancellationToken cancellationToken, bool timeoutEnabled = false)
    {
        BuildConsumer();
        try
        {
            Subscribe(cancellationToken);

            while (!ShouldStartConsuming() && !cancellationToken.IsCancellationRequested)
            {
                _logger.LogInformation($"Waiting for application to become ready before starting to consume from topics: {string.Join(" ,", ConsumerConfig.Topics)}");
                await Task.Delay(1000, cancellationToken);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                var consumeResult = Consume(cancellationToken, timeoutEnabled);
                    
                if (consumeResult.IsPartitionEOF)
                {
                    _logger.LogDebug($"Reached partition EOF for partition {consumeResult.Partition.Value}. Offset {consumeResult.Offset.Value}");
                    continue;
                }
                    
                if (!ShouldProcessMessage(consumeResult))
                    continue;
                    
                // Pass message rather than value so that the messageHandler has access to Headers
                await messageHandler(consumeResult.Message);

                HandleNotCommittedOffsets();
            }
        }
        finally
        {
            CommitOffset();
            DisposeCurrentConsumer();
        }
    }

    private ConsumeResult<TKey, TValue> Consume(CancellationToken cancellationToken, bool timeoutEnabled = false) 
        => timeoutEnabled ?
            Consumer.Consume(ConsumeTimeoutMs):
            Consumer.Consume(cancellationToken);
}