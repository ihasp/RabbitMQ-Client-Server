using System;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace GpuClient
{
    class Program
    {
        private const string QueueName = "gpu_processing_queue";

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== GPU Processing Client ===");
            
            try
            {
                var factory = new ConnectionFactory()
                {
                    HostName = "localhost",
                    UserName = "guest", 
                    Password = "guest",
                    VirtualHost = "/"
                };

                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                // Declare the main queue
                await channel.QueueDeclareAsync(queue: QueueName, durable: false, exclusive: false, autoDelete: false);

                // Create reply queue
                var replyQueueResult = await channel.QueueDeclareAsync(queue: "", durable: false, exclusive: true, autoDelete: false);
                var replyQueueName = replyQueueResult.QueueName;

                var consumer = new AsyncEventingBasicConsumer(channel);
                var correlationId = Guid.NewGuid().ToString();
                var tcs = new TaskCompletionSource<string>();

                consumer.ReceivedAsync += async (model, ea) =>
                {
                    try
                    {
                        if (ea.BasicProperties.CorrelationId == correlationId)
                        {
                            var response = Encoding.UTF8.GetString(ea.Body.ToArray());
                            tcs.SetResult(response);
                        }
                    }
                    catch (Exception ex)
                    {
                        tcs.SetException(ex);
                    }
                    await Task.CompletedTask;
                };

                await channel.BasicConsumeAsync(consumer: consumer, queue: replyQueueName, autoAck: true);

                // Get input from user
                Console.WriteLine("Podaj liczby całkowite oddzielone spacją (lub naciśnij Enter dla przykładu):");
                var input = Console.ReadLine();
                
                int[] numbers;
                if (string.IsNullOrWhiteSpace(input))
                {
                    numbers = new int[] { 1, 2, 3, 4, 5, 6, 7, 8 };
                    Console.WriteLine("Używam przykładowych danych: " + string.Join(", ", numbers));
                }
                else
                {
                    try
                    {
                        numbers = input.Split(' ', StringSplitOptions.RemoveEmptyEntries)
                                      .Select(int.Parse)
                                      .ToArray();
                    }
                    catch
                    {
                        Console.WriteLine("Nieprawidłowe dane, używam przykładu.");
                        numbers = new int[] { 1, 2, 3, 4, 5, 6, 7, 8 };
                    }
                }

                // Prepare message
                var message = JsonSerializer.Serialize(numbers);
                var body = Encoding.UTF8.GetBytes(message);

                var props = new BasicProperties
                {
                    CorrelationId = correlationId,
                    ReplyTo = replyQueueName
                };

                // Send request
                Console.WriteLine("Wysyłam żądanie do serwera GPU...");
                await channel.BasicPublishAsync(exchange: "", routingKey: QueueName, mandatory: false, basicProperties: props, body: body);

                // Wait for response with timeout
                var timeoutTask = Task.Delay(TimeSpan.FromSeconds(30));
                var completedTask = await Task.WhenAny(tcs.Task, timeoutTask);

                if (completedTask == timeoutTask)
                {
                    Console.WriteLine("Timeout - brak odpowiedzi z serwera w ciągu 30 sekund.");
                    return;
                }

                var resultJson = await tcs.Task;
                var result = JsonSerializer.Deserialize<int[]>(resultJson);

                Console.WriteLine("\n=== WYNIKI ===");
                Console.WriteLine("Dane wejściowe: " + string.Join(", ", numbers));
                Console.WriteLine("Wynik z GPU:    " + string.Join(", ", result ?? Array.Empty<int>()));
                Console.WriteLine("\nNaciśnij dowolny klawisz aby zakończyć...");
                Console.ReadKey();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd: {ex.Message}");
                Console.WriteLine("Upewnij się, że RabbitMQ jest uruchomiony na localhost:5672");
                Console.WriteLine("Naciśnij dowolny klawisz aby zakończyć...");
                Console.ReadKey();
            }
        }
    }
}