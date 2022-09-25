namespace KafkaCommon.Configuration;

/// <summary>
/// Configuration supporting setup of multi consumers and/or producers
/// </summary>
public class KafkaConfiguration
{
    public KafkaBaseSettings BaseConfig { get; set; }
    public Dictionary<string, TopicConfiguration> Consumers { get; set; }
    
    public Dictionary<string, TopicConfiguration> Producers { get; set; }
    //ToDo: Add more type specific configuration, for instance retry count for producers
}