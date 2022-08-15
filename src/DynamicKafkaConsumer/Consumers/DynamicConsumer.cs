namespace DynamicKafkaConsumer.Consumers;

using Contracts;
using KafkaCommon.Abstractions;
using KafkaCommon.Configuration;
using KafkaCommon.Services.DynamicConsumer;
using KafkaCommon.Services.Interfaces;
using Microsoft.Extensions.Options;

public class DynamicConsumer : AsyncDynamicConsumer<string, SportInfoMessage>
{
    public DynamicConsumer(
        IDynamicConsumerModifier<string> dynamicConsumerModifier,
        IOptionsMonitor<KafkaConfiguration> kafkaConfiguration,
        IConsumerEventsHandler? eventsHandler,
        IEnumerable<IMessageProcessor<string, SportInfoMessage>> messageProcessors,
        ILogger<DynamicConsumer> logger) 
        : base(dynamicConsumerModifier, kafkaConfiguration, eventsHandler, messageProcessors, logger)
    {
    }

    protected override string ConsumerConfigurationName() => nameof(DynamicConsumer);
}