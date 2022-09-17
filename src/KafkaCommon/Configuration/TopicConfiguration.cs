namespace KafkaCommon.Configuration;

public class TopicConfiguration
{
    public List<string> Topics { get; set; }
    
    public Dictionary<string, string> Settings { get; set; }
    
    public int MaxNotCommittedMessages { get; set; }

    public string HealthCheckTopic { get; set; } = Constants.HealthCheckTopicDefaultName;
    
    public bool IsHealthCheckEnabled { get; set; } = true;
}