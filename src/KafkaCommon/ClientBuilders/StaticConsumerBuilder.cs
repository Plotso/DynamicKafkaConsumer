namespace KafkaCommon.ClientBuilders;

using Confluent.Kafka;
using Abstractions;
using Configuration;

public static class StaticConsumerBuilder
{
    public static IConsumer<TKey, TMessage> BuildConsumer<TKey, TMessage>(
        TopicConfiguration config,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TMessage> valueDeserializer,
        IConsumerEventsHandler? eventsHandler) 
        => BuildConsumer(config.Settings, keyDeserializer, valueDeserializer, eventsHandler);

    public static IConsumer<TKey, TMessage> BuildConsumer<TKey, TMessage>(
        IEnumerable<KeyValuePair<string, string>> config,
        IDeserializer<TKey> keyDeserializer,
        IDeserializer<TMessage> valueDeserializer,
        IConsumerEventsHandler? eventsHandler)
    {
        var modifiedConfig = ReplaceConsumerGroupPlaceholders(config);
        
        var consumerBuilder = new ConsumerBuilder<TKey, TMessage>(modifiedConfig)
            .SetKeyDeserializer(keyDeserializer)
            .SetValueDeserializer(valueDeserializer);

        if (eventsHandler != null)
        {
            consumerBuilder = consumerBuilder
                .SetOffsetsCommittedHandler(eventsHandler.HandleOffsetsCommitted)
                .SetPartitionsAssignedHandler(eventsHandler.HandlePartitionsAssigned)
                .SetPartitionsRevokedHandler(eventsHandler.HandlePartitionsRevoked)
                .SetErrorHandler((c, e) =>
                {
                    if (e.IsFatal)
                    {
                        eventsHandler.HandleFatalError(c, e);
                        return;
                    }

                    eventsHandler.HandleError(c, e);
                });
        }

        return consumerBuilder.Build();
    }
    
    
    private static IEnumerable<KeyValuePair<string, string>> ReplaceConsumerGroupPlaceholders(IEnumerable<KeyValuePair<string, string>> config) {
        const string GroupIdKey = "group.id";
        var newConfig =
            config.ToDictionary(s => s.Key, s => s.Value);

        if (newConfig.TryGetValue(GroupIdKey, out var value))
            newConfig[GroupIdKey] = FillInVariableValues(value);

        return newConfig;
    }
    
    private static string FillInVariableValues(string value) 
        => value.Replace("[Guid]", Guid.NewGuid().ToString("n").Substring(0, 13));
}