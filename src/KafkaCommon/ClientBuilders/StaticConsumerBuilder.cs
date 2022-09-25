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
        IConsumerEventsHandler? eventsHandler,
        string configurationSectionName)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics, configurationSectionName);
        if (keyDeserializer != null)
            consumerBuilder.SetKeyDeserializer(keyDeserializer);
        if (valueDeserializer != null)
            consumerBuilder.SetValueDeserializer(valueDeserializer);
        
        return consumerBuilder;
    }
    
    public static IConsumer<TKey, TMessage> BuildConsumer<TKey, TMessage>(
        TopicConfiguration config,
        IDeserializer<TKey>? keyDeserializer,
        IDeserializer<TMessage>? valueDeserializer,
        IConsumerEventsHandler? eventsHandler,
        string configurationSectionName)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics, configurationSectionName);
        if (keyDeserializer != null)
            consumerBuilder.SetKeyDeserializer(keyDeserializer);
        if (valueDeserializer != null)
            consumerBuilder.SetValueDeserializer(valueDeserializer);
        if (eventsHandler != null)
            consumerBuilder.AddEventHandlers(eventsHandler);
        
        return consumerBuilder.Build();
    }
    
    public static IConsumer<TKey, TMessage> BuildConsumerWithDefaultDeserializers<TKey, TMessage>(
        TopicConfiguration config,
        IConsumerEventsHandler? eventsHandler,
        string configurationSectionName)
    {
        var consumerBuilder = new ConsumerBuilderTopic<TKey, TMessage>(config.Settings, config.Topics, configurationSectionName);
        if (eventsHandler != null)
            consumerBuilder.AddEventHandlers(eventsHandler);
        return consumerBuilder.Build();
    }
}