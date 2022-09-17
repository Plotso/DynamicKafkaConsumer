namespace KafkaCommon.Services.Consumers.Interfaces;
public interface IPartitionConsumer<TKey, TValue>
{
    void Subscribe(CancellationToken cancellationToken);
    void Unsubscribe(CancellationToken cancellationToken);
}