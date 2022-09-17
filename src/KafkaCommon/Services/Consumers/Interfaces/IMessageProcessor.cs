namespace KafkaCommon.Services.Consumers.Interfaces;

using Confluent.Kafka;

public interface IMessageProcessor<TKey, TMessage>
{
    /// <summary>
    /// Indicating whether processor is ready to operate or it needs some time
    /// </summary>
    Task<bool> ReadyForProcessingMessages(CancellationToken cancellationToken = default);

    ValueTask<bool> ShouldProcessMessage(ConsumeResult<TKey, TMessage> consumeResult, CancellationToken cancellationToken = default);

    Task<bool> TryProcessMessage(ConsumeResult<TKey, TMessage> consumeResult, CancellationToken cancellationToken = default);
}