namespace KafkaCommon.Services.Consumers.BasicConsumer;

using Confluent.Kafka;
using KafkaCommon.Abstractions;
using KafkaCommon.Serialization;

public static class BasicConsumerBuilder
{
    public const string GroupIdGuidPlaceholder = "[Guid]";

    /// <summary>
    /// Creates basic consumer without the extended logic for different kafka event handler
    /// </summary>
    public static IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(ConsumerConfig config)
        where TValue : class
    {
        config.GroupId =
            FillInVariableValues(config.GroupId); // if group.id is set to [Guid] it will be replaced with actual guid
        return new ConsumerBuilder<TKey, TValue>(config)
            .SetValueDeserializer(new JsonValueSerializer<TValue>())
            .Build();
    }

    /// <summary>
    /// Creates enhanced consumer with extended logic for different kafka event handlers
    /// </summary>
    public static IConsumer<TKey, TValue> CreateConsumer<TKey, TValue>(ConsumerConfig config,
        IConsumerEventsHandler consumerEventsHandler, JsonValueSerializer<TValue> serializer)
        where TValue : class
    {
        config.GroupId =
            FillInVariableValues(config.GroupId); // if group.id is set to [Guid] it will be replaced with actual guid
        return new ConsumerBuilder<TKey, TValue>(config)
            .WithEventHandlers(consumerEventsHandler)
            .SetValueDeserializer(serializer)
            .Build();
    }


    private static ConsumerBuilder<TKey, TMessage> WithEventHandlers<TKey, TMessage>(
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

    /// <summary>
    ///  Replaces [Guid] placeholder with actual guid
    /// </summary>
    private static string FillInVariableValues(string value)
        => value.Replace(GroupIdGuidPlaceholder, Guid.NewGuid().ToString("n").Substring(0, 13));
}