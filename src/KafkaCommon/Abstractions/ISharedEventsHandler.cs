namespace KafkaCommon.Abstractions;

using Confluent.Kafka;

public interface ISharedEventsHandler
{
    void HandleError(IClient client, Error error);

    void HandleFatalError(IClient client, Error error);
}