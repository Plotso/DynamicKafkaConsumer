namespace DynamicKafkaConsumer.Consumers.BasicConsumers;

using Confluent.Kafka;
using Contracts;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using KafkaCommon.Serialization;
using KafkaCommon.Services.Consumers.BasicConsumer;
using Microsoft.Extensions.Options;

public class ExampleBasicConsumer : BasicConsumer<string, SportInfoMessage>
{
    public ExampleBasicConsumer(
        IOptionsMonitor<KafkaConfiguration> config,
        IConsumerEventsHandler consumerEventsHandler,
        JsonValueSerializer<SportInfoMessage> serializer,
        ILogger<ExampleBasicConsumer> logger) 
        : base(config, consumerEventsHandler, serializer, logger)
    {
    }

    protected override string ConsumerConfigurationName => nameof(ExampleBasicConsumer);

    protected override bool ShouldProcessMessage(ConsumeResult<string, SportInfoMessage> consumeResult)
        => consumeResult?.Message?.Value != null;
}