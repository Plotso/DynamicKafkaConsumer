namespace KafkaCommon.Abstractions;

using Confluent.Kafka;

public interface IConsumerEventsHandler : ISharedEventsHandler
{
    void HandleOffsetsCommitted(IClient client, CommittedOffsets offsets);

    IEnumerable<TopicPartitionOffset> HandlePartitionsAssigned(IClient client, List<TopicPartition> topicPartitions);

    IEnumerable<TopicPartitionOffset> HandlePartitionsRevoked(IClient client, List<TopicPartitionOffset> topicPartitionOffsets);
}