using Confluent.Kafka;

var config = new ConsumerConfig
{
    BootstrapServers = "localhost:9092",
    GroupId = "test-consumer-group",
    AutoOffsetReset = AutoOffsetReset.Earliest,
    EnableAutoCommit = false // Отключаем авто-коммит для ручного управления
};

using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();
consumer.Subscribe("test-topic");

try
{
    while (true)
    {
        try
        {
            var consumeResult = consumer.Consume(CancellationToken.None);
                    
            Console.WriteLine($"Получено сообщение: {consumeResult.Message.Value}");
            Console.WriteLine($"Партиция: {consumeResult.Partition}, Оффсет: {consumeResult.Offset}");

            // Вручную подтверждаем обработку сообщения
            consumer.Commit(consumeResult);
        }
        catch (ConsumeException e)
        {
            Console.WriteLine($"Ошибка потребления: {e.Error.Reason}");
        }
    }
}
catch (OperationCanceledException)
{
    // Graceful shutdown
    consumer.Close();
}