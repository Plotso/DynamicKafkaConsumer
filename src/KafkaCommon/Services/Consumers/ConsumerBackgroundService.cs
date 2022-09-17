namespace KafkaCommon.Services.Consumers;

using Confluent.Kafka;
using KafkaCommon.ClientBuilders;
using KafkaCommon.Configuration;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

/// <summary>
/// Contains base logic required to use when starting a normal consumer.
/// </summary>
public abstract class ConsumerBackgroundService<TKey, TValue> : IHostedService, IDisposable
{
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public ConsumerBackgroundService(ConsumerBuilderTopic<TKey, TValue> builder, IOptionsMonitor<KafkaConfiguration> kafkaConfiguration)
    {
        Builder = builder;
        _kafkaConfiguration = kafkaConfiguration;
    }
    
    protected readonly ConsumerBuilderTopic<TKey, TValue> Builder;
    private readonly IOptionsMonitor<KafkaConfiguration> _kafkaConfiguration;

    protected CancellationToken StoppingToken
        => _cts.Token;

    protected IConsumer<TKey, TValue> Consumer { get; set; }
    protected IConsumer<TKey, byte[]> ByteConsumer { get; set; }
        
    protected int NotCommittedMessagesCount { get; set; }

    protected virtual bool ManuallyHandleOffsetCommit => false;
    public List<TopicPartition> PartitionsAssigned() => Consumer.Assignment;

    public abstract Task StartAsync(CancellationToken cancellationToken);

    public virtual Task StopAsync(CancellationToken cancellationToken)
    {
        Dispose();
        return Task.CompletedTask;
    }

    public void Dispose()
    {
        try 
        {
            _cts.Cancel();
            Consumer?.Close();
            Consumer?.Dispose();
        } catch (ObjectDisposedException)  { /* suppress */ }
    }

    protected void BuildConsumer() => Consumer = Builder.Build();

    public void ManuallyCommitOffset(ConsumeResult<TKey, TValue> consumeResult) => Consumer.Commit(consumeResult);

    public void ManuallyCommitOffset(IEnumerable<TopicPartitionOffset> offsets) => Consumer.Commit(offsets);
    
    // Executes CommitOffset but suppresses any potential exception
    protected void SafeCommitOffset()
    {
        try
        {
            CommitOffset();
        }
        catch (Exception){ /* suppress */ }
    }

    protected void CommitOffset()
    {
        if (ManuallyHandleOffsetCommit)
            return;
            
        if (NotCommittedMessagesCount > 0)
            Consumer.Commit();
    }

    protected void HandleNotCommittedOffsets()
    {
        NotCommittedMessagesCount++;
        if (ManuallyHandleOffsetCommit)
            return;
            
        if (NotCommittedMessagesCount >= _kafkaConfiguration.CurrentValue.Consumers[Builder.ConfigurationSectionName].MaxNotCommittedMessages)
        {
            Consumer.Commit();
            NotCommittedMessagesCount = 0;
        }
    }
}