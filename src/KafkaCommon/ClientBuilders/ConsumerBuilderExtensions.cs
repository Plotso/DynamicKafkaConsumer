namespace KafkaCommon.ClientBuilders;

using Abstractions;
using Confluent.Kafka;

public static class ConsumerBuilderExtensions
{
    public static ConsumerBuilder<TKey, TMessage> WithEventHandlers<TKey, TMessage>(
        this ConsumerBuilder<TKey, TMessage> consumerBuilder, IConsumerEventsHandler? eventsHandler)
    {
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

        return consumerBuilder;
    }

    public static void AddEventHandlers<TKey, TMessage>(this ConsumerBuilder<TKey, TMessage> consumerBuilder, IConsumerEventsHandler? eventsHandler) 
        => consumerBuilder.WithEventHandlers(eventsHandler);
}