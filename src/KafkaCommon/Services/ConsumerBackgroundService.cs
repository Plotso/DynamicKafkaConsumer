namespace KafkaCommon.Services;

using Abstractions;
using ClientBuilders;
using Confluent.Kafka;
using Microsoft.Extensions.Hosting;

/// <summary>
/// Contains base logic required to use when starting a normal consumer.
/// </summary>
public abstract class ConsumerBackgroundService<TKey, TValue> : IHostedService, IDisposable
{
    private readonly CancellationTokenSource _cts = new CancellationTokenSource();

    public ConsumerBackgroundService(ConsumerBuilderTopic<TKey, TValue> builder)
    {
        Builder = builder;
    }
    
    protected readonly ConsumerBuilderTopic<TKey, TValue> Builder;
    
    protected CancellationToken StoppingToken
        => _cts.Token;

    protected IConsumer<TKey, TValue> Consumer { get; set; }
    protected IConsumer<TKey, byte[]> ByteConsumer { get; set; }

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
}