using System.Text;
using System.Text.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using ILGPU;
using ILGPU.Runtime;
using ILGPU.Runtime.OpenCL;

namespace Server
{
    class Program
    {
        private const string QueueName = "gpu_processing_queue";
        private static Context? _context;
        private static Accelerator? _accelerator;

        // GPU Kernel - multiplies each element by 2
        static void MultiplyKernel(Index1D index, ArrayView<int> data)
        {
            data[index] *= 2;
        }

        static async Task Main(string[] args)
        {
            Console.WriteLine("=== GPU Processing Server ===");
            
            try
            {
                // Initialize ILGPU
                InitializeGpu();

                // Initialize RabbitMQ
                var factory = new ConnectionFactory()
                {
                    HostName = "localhost",
                    UserName = "guest",
                    Password = "guest", 
                    VirtualHost = "/"
                };

                using var connection = await factory.CreateConnectionAsync();
                using var channel = await connection.CreateChannelAsync();

                await channel.QueueDeclareAsync(queue: QueueName, durable: false, exclusive: false, autoDelete: false);

                var consumer = new AsyncEventingBasicConsumer(channel);

                consumer.ReceivedAsync += async (model, ea) =>
                {
                    try
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        
                        Console.WriteLine($"Otrzymano żądanie: {message}");

                        // Deserialize input
                        var numbers = JsonSerializer.Deserialize<int[]>(message);
                        if (numbers == null || numbers.Length == 0)
                        {
                            Console.WriteLine("Nieprawidłowe dane wejściowe");
                            return;
                        }

                        // Process on GPU
                        var result = await ProcessOnGPU(numbers);

                        // Send response
                        var responseMessage = JsonSerializer.Serialize(result);
                        var responseBody = Encoding.UTF8.GetBytes(responseMessage);

                        var props = new BasicProperties
                        {
                            CorrelationId = ea.BasicProperties.CorrelationId
                        };

                        await channel.BasicPublishAsync(
                            exchange: "",
                            routingKey: ea.BasicProperties.ReplyTo ?? "",
                            mandatory: false,
                            basicProperties: props,
                            body: responseBody);

                        Console.WriteLine($"Wysłano odpowiedź: {responseMessage}");
                        
                        await channel.BasicAckAsync(deliveryTag: ea.DeliveryTag, multiple: false);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Błąd przetwarzania żądania: {ex.Message}");
                        await channel.BasicNackAsync(deliveryTag: ea.DeliveryTag, multiple: false, requeue: false);
                    }
                };

                await channel.BasicConsumeAsync(consumer: consumer, queue: QueueName, autoAck: false);

                Console.WriteLine("Serwer GPU uruchomiony. Oczekiwanie na żądania...");
                Console.WriteLine("Naciśnij [Ctrl+C] aby zatrzymać serwer");

                // Keep the server running
                var cancellationToken = new CancellationTokenSource();
                Console.CancelKeyPress += (sender, e) =>
                {
                    e.Cancel = true;
                    cancellationToken.Cancel();
                };

                try
                {
                    await Task.Delay(Timeout.Infinite, cancellationToken.Token);
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("\nZatrzymywanie serwera...");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd serwera: {ex.Message}");
            }
            finally
            {
                CleanupGpu();
                Console.WriteLine("Serwer zatrzymany.");
            }
        }

        static void InitializeGpu()
        {
            try
            {
                // Create ILGPU context with OpenCL and CPU support
                _context = Context.Create(builder => builder.OpenCL());

                // Try to get OpenCL device first, fallback to CPU
                var openclDevices = _context.Devices.Where(d => d.AcceleratorType == AcceleratorType.OpenCL).ToArray();
                
                if (openclDevices.Length > 0)
                {
                    _accelerator = openclDevices[0].CreateAccelerator(_context);
                    Console.WriteLine($"Używam OpenCL GPU: {_accelerator.Name}");
                }
                else
                {
                    var cpuDevice = _context.Devices.FirstOrDefault(d => d.AcceleratorType == AcceleratorType.CPU);
                    if (cpuDevice != null)
                    {
                        _accelerator = cpuDevice.CreateAccelerator(_context);
                        Console.WriteLine($"Używam CPU: {_accelerator.Name}");
                    }
                    else
                    {
                        throw new InvalidOperationException("Brak dostępnych urządzeń obliczeniowych");
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Błąd inicjalizacji GPU: {ex.Message}");
                throw;
            }
        }

        static async Task<int[]> ProcessOnGPU(int[] input)
        {
            if (_accelerator == null)
                throw new InvalidOperationException("GPU nie został zainicjalizowany");

            return await Task.Run(() =>
            {
                // Load kernel
                var kernel = _accelerator.LoadAutoGroupedStreamKernel<Index1D, ArrayView<int>>(MultiplyKernel);

                // Allocate GPU buffer
                using var buffer = _accelerator.Allocate1D<int>(input.Length);

                // Copy data to GPU
                buffer.CopyFromCPU(input);

                // Execute kernel
                kernel(new Index1D(input.Length), buffer.View);

                // Wait for completion
                _accelerator.Synchronize();

                // Get result
                return buffer.GetAsArray1D();
            });
        }

        static void CleanupGpu()
        {
            _accelerator?.Dispose();
            _context?.Dispose();
        }
    }
}