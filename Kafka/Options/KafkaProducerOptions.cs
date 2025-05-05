namespace Options;

public class KafkaProducerOptions
{
    public required string BootstrapServers { get; set; }
    public required string ClientId { get; set; }
    public required string Topic { get; set; }
}