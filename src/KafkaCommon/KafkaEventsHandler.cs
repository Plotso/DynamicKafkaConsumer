namespace KafkaCommon;

using Abstractions;
using Confluent.Kafka;
using Extensions;
using Microsoft.Extensions.Logging;

public class KafkaEventsHandler : IConsumerEventsHandler // IConsumerEventsHandler also brings ISharedEventsHandler
{
    private readonly ILogger<KafkaEventsHandler> _logger;

    public KafkaEventsHandler(ILogger<KafkaEventsHandler> logger)
    {
        _logger = logger;
    }
    public void HandleError(IClient client, Error error)
    {
        SendErrorData(nameof(HandleError), error);
        ExitOnFatalErrors(error);
    }

    public void HandleFatalError(IClient client, Error error) 
        => HandleError(client, error);

    public void HandleOffsetsCommitted(IClient client, CommittedOffsets offsets)
    {
        if (offsets.Error == null || !offsets.Error.IsError)
            return;
        
        var error = offsets.Error.ToLogMessage();
        error = offsets.Offsets.Aggregate(error, 
            (current, item) 
                => current + $"Partition: {item.Partition} {Environment.NewLine}" + $"Offset: {item.Offset} {Environment.NewLine}");

        SendErrorData(nameof(HandleOffsetsCommitted), error);
    }

    public IEnumerable<TopicPartitionOffset> HandlePartitionsAssigned(IClient client, List<TopicPartition> topicPartitions)
    {
        var information = AggregateInformation(
            $"Partitions Assigned.{Environment.NewLine}",
            topicPartitions,
            (currentInfo, offset) => currentInfo + $"{offset}{Environment.NewLine}");

        SendInfoData(nameof(HandlePartitionsRevoked), information);
        return Enumerable.Empty<TopicPartitionOffset>();
    }

    public IEnumerable<TopicPartitionOffset> HandlePartitionsRevoked(IClient client, List<TopicPartitionOffset> topicPartitionOffsets)
    {
        var information = AggregateInformation(
            $"Partitions HandlePartitionsRevoked.{Environment.NewLine}",
            topicPartitionOffsets,
            (currentInfo, offset) => currentInfo + $"{offset}{Environment.NewLine}");

        SendInfoData(nameof(HandlePartitionsRevoked), information);
        return Enumerable.Empty<TopicPartitionOffset>();
    }

    /// <summary>
    /// Get aggregated information data from provided collection to the informationPreface based on the aggregation function passed
    /// </summary>
    private string AggregateInformation<T>(
        string informationPreface,
        IEnumerable<T> collectionToAggregate,
        Func<string, T, string> textAggregationEntry) 
        => collectionToAggregate.Aggregate(informationPreface, textAggregationEntry);

    private void ExitOnFatalErrors(Error error)
    {
        if (error.Code != ErrorCode.Local_AllBrokersDown && error.Code != ErrorCode.Local_Fatal) 
            return;
        _logger.LogCritical($"Terminating application due to {error.Code} error");
        Environment.Exit(1);
    }

    private void SendInfoData(string actionName, string information)
    {
        _logger.LogInformation(information);
        SendMetrics(actionName);
    }

    private void SendErrorData(string actionName, Error error) 
        => SendErrorData(actionName, error.ToLogMessage());

    private void SendErrorData(string actionName, string error)
    {
        _logger.LogError(error);
        SendMetrics(actionName);
    }

    private void SendMetrics(string metricName)
    {
        // If your app is using metrics, add your logic here 
    }
}