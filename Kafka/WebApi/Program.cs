using System.Text.Json;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;
using Models;

var builder = WebApplication.CreateBuilder(args);


var app = builder.Build();

if (app.Environment.IsDevelopment())
{
}

app.UseHttpsRedirection();

var config = new ProducerConfig
{
    BootstrapServers = "localhost:9092",
    ClientId = "my-producer-app"
};

app.MapPost("/users", async ([FromBody] User user) =>
{
    using var producer = new ProducerBuilder<string, string>(config)
        .Build();
    
    var topic = "test-topic";
    var message = new Message<string, string>
    {
        Key = "message-key",
        Value = JsonSerializer.Serialize(user)
    };
    
    var deliveryResult = await producer.ProduceAsync(topic, message);
            
    Console.WriteLine($"Delivered message to: {deliveryResult.TopicPartitionOffset}");
    Console.WriteLine($"Partition: {deliveryResult.Partition}, Offset: {deliveryResult.Offset}");
});


app.Run();