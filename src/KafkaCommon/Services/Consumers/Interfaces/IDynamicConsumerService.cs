namespace KafkaCommon.Services.Consumers.Interfaces;

using Microsoft.Extensions.Hosting;

public interface IDynamicConsumerService : IHostedService, IDisposable
{
    /// <summary>
    /// Indicator whether IDynamicConsumerService has currently initialised a consumer
    /// </summary>
    /// <returns></returns>
    bool IsConsumerSet();
}