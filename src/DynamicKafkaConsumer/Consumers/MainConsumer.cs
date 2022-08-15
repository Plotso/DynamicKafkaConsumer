namespace DynamicKafkaConsumer.Consumers;

using Contracts;
using KafkaCommon.Abstractions;
using KafkaCommon.ClientBuilders;
using KafkaCommon.Services;
using KafkaCommon.Services.Interfaces;

public class MainConsumer : AsyncConsumer<string, SportInfoMessage>
{
    public MainConsumer(
        ConsumerBuilderTopic<string, SportInfoMessage> builder,
        IEnumerable<IMessageProcessor<string, SportInfoMessage>> messageProcessors,
        ILogger<MainConsumer> logger) 
        : base(builder, messageProcessors, logger)
    {
    }
}