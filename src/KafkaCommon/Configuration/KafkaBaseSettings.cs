namespace KafkaCommon.Configuration;

public class KafkaBaseSettings
{
    public List<string> Topics { get; set; }
    public Dictionary<string, string> BaseSettings { get; set; }
}