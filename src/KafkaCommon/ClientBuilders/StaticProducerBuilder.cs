namespace KafkaCommon.ClientBuilders;

using Abstractions;
using Configuration;
using Confluent.Kafka;

public class StaticProducerBuilder
{
    public static IProducer<TKey, TMessage> BuildProducer<TKey, TMessage>(
        TopicConfiguration config,
        ISerializer<TKey> keySerializer,
        ISerializer<TMessage> valueSerializer,
        ISharedEventsHandler? eventsHandler) 
        => BuildProducer(config.Settings, keySerializer, valueSerializer, eventsHandler);
    public static IProducer<TKey, TMessage> BuildProducer<TKey, TMessage>(
        IEnumerable<KeyValuePair<string, string>> config,
        ISerializer<TKey> keySerializer,
        ISerializer<TMessage> valueSerializer,
        ISharedEventsHandler? eventsHandler)
    {
        var producerBuilder = new ProducerBuilder<TKey, TMessage>(config)
            .SetKeySerializer(keySerializer)
            .SetValueSerializer(valueSerializer);

        if (eventsHandler != null)
        {
            producerBuilder = producerBuilder
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

        return producerBuilder.Build();
    }
}