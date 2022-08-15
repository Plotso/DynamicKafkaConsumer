namespace DynamicKafkaConsumer.Contracts;

public class SportInfo
{
    public int SportId { get; set; }
    
    public int? EventId { get; set; }
    
    public string Info { get; set; }
}