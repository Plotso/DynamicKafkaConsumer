namespace KafkaCommon.ClientBuilders;

using Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

[Obsolete]
public class KafkaClientBuilder
{
    private readonly IOptionsMonitor<KafkaConfiguration> _kafkaConfig;
    private readonly ILogger _logger;

    public KafkaClientBuilder(IOptionsMonitor<KafkaConfiguration> kafkaConfig, ILogger logger)
    {
        _kafkaConfig = kafkaConfig;
        _logger = logger;
    }
}