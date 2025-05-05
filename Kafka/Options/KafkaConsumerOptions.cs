namespace Options;

public class KafkaConsumerOptions
{
    public required string BootstrapServers { get; set; }
    public required string ClientId { get; set; }
    public required string GroupId  { get; set; }
    public required string Topic { get; set; }
}