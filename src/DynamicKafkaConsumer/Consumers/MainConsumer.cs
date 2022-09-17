namespace DynamicKafkaConsumer.Consumers;

using Contracts;
using KafkaCommon.Abstractions;
using KafkaCommon.ClientBuilders;
using KafkaCommon.Configuration;
using KafkaCommon.Services;
using KafkaCommon.Services.Consumers;
using KafkaCommon.Services.Consumers.Interfaces;
using Microsoft.Extensions.Options;

public class MainConsumer : AsyncConsumer<string, SportInfoMessage>
{
    public MainConsumer(
        ConsumerBuilderTopic<string, SportInfoMessage> builder,
        IEnumerable<IMessageProcessor<string, SportInfoMessage>> messageProcessors,
        IOptionsMonitor<KafkaConfiguration> kafkaConfiguration,
        ILogger<MainConsumer> logger) 
        : base(builder, messageProcessors, kafkaConfiguration, logger)
    {
    }
}