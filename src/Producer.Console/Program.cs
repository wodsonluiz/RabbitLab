using RabbitMQ.Client;
using Serilog;
using System;
using System.Collections.Generic;
using System.Text;

namespace Producer.Console
{
    public class Program
    {
        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();

            logger.Information($"Começando a publicação da mensagem");
            var exchangeName = "ProducerTestExchange2";
            var queueName = "TestWodQueueWithArguments";

            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqp://mark1-api:v7IfmfZm5gPDzg==@localhost:5672/Staging-Mark1")
            };

            try
            {
                using (var conn = factory.CreateConnection())
                {
                    using (var channel = conn.CreateModel())
                    {
                        var message = Encoding.UTF8.GetBytes($"Mensagem teste {Guid.NewGuid()}");
                        var props = channel.CreateBasicProperties();
                        props.ContentType = "text/plain";
                        props.DeliveryMode = 2;
                        props.Headers = new Dictionary<string, object>
                        {
                            { "x-match", "all" },
                            { "headerKey", Guid.NewGuid().ToString() },
                            {"Property2","Valor da property 4"},
                        };


                        // channel.QueueDeclare(queue: queueName,
                        //                          durable: false,
                        //                          exclusive: false,
                        //                          autoDelete: false,
                        //                          arguments: null);

                        channel.BasicPublish(exchange: exchangeName,
                                            routingKey: "mensagem.*",
                                            basicProperties: props,
                                            body: message);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Error($"Error: {ex.Message}");
            }
            finally
            {
                logger.Information($"Finalizando a publicação da mensagem");
            }
        }
    }
}


