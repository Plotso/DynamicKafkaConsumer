namespace KafkaCommon.ClientBuilders;

using Confluent.Kafka;
using Abstractions;
using Configuration;

public static class StaticConsumerBuilder
{
    
    public static ConsumerBuilderTopic<TKey, TMessage> AddConsumerBuilder<TKey, TMessage>(
        TopicConfiguration config,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TMessage>? valueDeserializer,
        IConsumerEventsHandler? eventsHandler)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics);
        if (keyDeserializer != null)
            consumerBuilder.SetKeyDeserializer(keyDeserializer);
        if (valueDeserializer != null)
            consumerBuilder.SetValueDeserializer(valueDeserializer);

        consumerBuilder.AddEventHandlers(eventsHandler);
        return consumerBuilder;
    }
    
    public static IConsumer<TKey, TMessage> BuildConsumer<TKey, TMessage>(
        TopicConfiguration config,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TMessage>? valueDeserializer,
        IConsumerEventsHandler? eventsHandler)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics);
        if (keyDeserializer != null)
            consumerBuilder.SetKeyDeserializer(keyDeserializer);
        if (valueDeserializer != null)
            consumerBuilder.SetValueDeserializer(valueDeserializer);

        consumerBuilder.AddEventHandlers(eventsHandler);
        return consumerBuilder.Build();
    }
    
    public static IConsumer<TKey, TMessage> BuildConsumerWithDefaultDeserializers<TKey, TMessage>(
        TopicConfiguration config,
        IConsumerEventsHandler? eventsHandler)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics);
        consumerBuilder.AddEventHandlers(eventsHandler);
        return consumerBuilder.Build();
    }
}