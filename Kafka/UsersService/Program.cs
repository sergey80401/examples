using System.Text.Json;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Models;
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
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe(kafkaConsumerOptions.Topic);

var users = new List<User>();

try
{
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume(CancellationToken.None);

            users.Add(JsonSerializer.Deserialize<User>(consumeResult.Message.Value) ??
                      throw new InvalidOperationException("Failed to get user"));

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