namespace DynamicKafkaConsumer.Consumers.BasicConsumers;

using Confluent.Kafka;
using Contracts;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using KafkaCommon.Serialization;
using KafkaCommon.Services.Consumers.BasicConsumer;
using KafkaCommon.Services.Consumers.Interfaces;
using Microsoft.Extensions.Options;

public class ExampleBasicDynamicConsumer : BasicDynamicConsumer<string, SportInfoMessage>
{
    public ExampleBasicDynamicConsumer(
        IOptionsMonitor<KafkaConfiguration> config,
        IConsumerEventsHandler consumerEventsHandler,
        IDynamicConsumerModifier<string> dynamicConsumerModifier,
        JsonValueSerializer<SportInfoMessage> serializer, 
        ILogger<ExampleBasicDynamicConsumer> logger) 
        : base(config, consumerEventsHandler, dynamicConsumerModifier, serializer, logger)
    {
    }
    
    protected override int CheckingForKeysConfigurationDelay => 100;

    protected override string ConsumerConfigurationName => nameof(ExampleBasicDynamicConsumer);
    protected override bool ShouldProcessMessage(ConsumeResult<string, SportInfoMessage> consumeResult)
        => consumeResult?.Message?.Value != null;
}