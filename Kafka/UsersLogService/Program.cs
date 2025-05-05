using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Options;

var configuration = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .Build();

var kafkaConsumerOptions = configuration.GetSection("KafkaConsumerOptions").Get<KafkaConsumerOptions>() ??
                           throw new InvalidOperationException("Failed to get settings to connect to Kafka");

var config = new ConsumerConfig
{
    BootstrapServers = kafkaConsumerOptions.BootstrapServers,
    GroupId = kafkaConsumerOptions.GroupId,
    ClientId = kafkaConsumerOptions.ClientId,
    AutoOffsetReset = AutoOffsetReset.Latest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(kafkaConsumerOptions.Topic);

try
{
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume(CancellationToken.None);
                    
            Console.WriteLine($"Message received: {consumeResult.Message.Value}");
            Console.WriteLine($"Partition: {consumeResult.Partition}, Offset: {consumeResult.Offset}");

            consumer.Commit(consumeResult);
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Consumption error: {e.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    consumer.Close();
}