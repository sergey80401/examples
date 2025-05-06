using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;
using Models;
using Options;

var builder = WebApplication.CreateBuilder(args);

var configuration = new ConfigurationBuilder()
    .AddEnvironmentVariables()
    .Build();


builder.Services.Configure<KafkaProducerOptions>(options =>
{
    var kafkaProducerOptions = configuration.GetSection("KafkaProducerOptions").Get<KafkaProducerOptions>() ??
                               throw new InvalidOperationException("Failed to get settings to connect to Kafka");

    options.BootstrapServers = kafkaProducerOptions.BootstrapServers;
    options.ClientId = kafkaProducerOptions.ClientId;
    options.Topic = kafkaProducerOptions.Topic;
});

var app = builder.Build();

app.UseHttpsRedirection();

app.MapPost("/users", async ([FromBody] User user, IOptions<KafkaProducerOptions> producerOptions) =>
{
    var pOptions = producerOptions.Value;

    var config = new ProducerConfig
    {
        BootstrapServers = pOptions.BootstrapServers,
        ClientId = pOptions.ClientId
    };

    using var producer = new ProducerBuilder<string, string>(config)
        .Build();

    var message = new Message<string, string>
    {
        Key = "user:created",
        Value = JsonSerializer.Serialize(user),
    };

    var deliveryResult = await producer.ProduceAsync(pOptions.Topic, message);

    Console.WriteLine($"Delivered message to: {deliveryResult.TopicPartitionOffset}");
    Console.WriteLine($"Partition: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");
});

app.Run();