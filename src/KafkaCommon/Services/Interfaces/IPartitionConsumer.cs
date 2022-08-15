namespace KafkaCommon.Services.Interfaces;
public interface IPartitionConsumer<TKey, TValue>
{
    void Subscribe(CancellationToken cancellationToken);
    void Unsubscribe(CancellationToken cancellationToken);
}