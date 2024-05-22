using RabbitMQ.Client.Events;
using RabbitMQ.Client;
using Serilog;
using Serilog.Core;
using System;
using System.Text;
using System.Collections.Generic;

namespace Consumer.Console
{
    public class Program
    {

        static void Main(string[] args)
        {
            var logger = new LoggerConfiguration()
                    .WriteTo.Console()
                    .CreateLogger();

            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqp://mark1-api:v7IfmfZm5gPDzg==@localhost:5672/Staging-Mark1")
            };

            logger.Information("** Começando a consumir mensagens **");

            HandlerConsumerWithHeaders(logger: logger, 
                factory: factory, 
                queueName: "Orders-Poi", 
                exchangeName: "Mark1.Headers", 
                routingKey: "orders.#",
                keyHeader: "Transaction-From-Mark1");

            HandlerConsumer(logger: logger, 
                factory: factory, 
                queueName: "Orders", 
                exchangeName: "Mark1");


            logger.Information("** Finaliznando o consumode de mensagens **");
        }

        static void HandlerConsumer(Logger logger, 
            ConnectionFactory factory, 
            string queueName, 
            string exchangeName)
        {
            using (var conn = factory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        logger.Information($"Estou recebendo na exchange {exchangeName}, valor da mensagem: {message}");
                    };

                    channel.BasicConsume(queue: queueName,
                                         autoAck: true,
                                         arguments: null,
                                         consumer: consumer);

                    string consumerTag = channel.BasicConsume(queueName, true, consumer);
                }
            }
        }

        public static void HandlerConsumerWithHeaders(Logger logger, 
            ConnectionFactory factory, 
            string queueName, 
            string exchangeName, 
            string routingKey, 
            string keyHeader)
        {
            using (var conn = factory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    var headers = new Dictionary<string, object>
                    {
                        { "x-match", "all" },
                        { "headerKey", Guid.NewGuid().ToString() },
                        { "Transaction-Poi", keyHeader },
                    };

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        logger.Information($"Estou recebendo com exchange na exchange {exchangeName}, valor da mensagem: {message}");
                    };

                    channel.BasicConsume(queue: queueName,
                                         autoAck: true,
                                         arguments: headers,
                                         consumer: consumer);

                    string consumerTag = channel.BasicConsume(queueName, true, consumer);
                }
            }
        }

        public static void HandlerConsumer(Logger logger, ConnectionFactory factory, string queueName)
        {
            using (var conn = factory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    channel.QueueDeclare(queue: queueName,
                                         durable: false,
                                         exclusive: false,
                                         autoDelete: false,
                                         arguments: null);

                    var consumer = new EventingBasicConsumer(channel);

                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        logger.Information($"Mensagem recebida: {message}");
                    };

                    channel.BasicConsume(queue: queueName,
                                         autoAck: true,
                                         consumer: consumer);
                }
            }
        }
    }
}


