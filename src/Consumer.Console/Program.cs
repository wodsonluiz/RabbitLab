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

            logger.Information("** Começando a consumir mensagens **");

            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqp://mark1-api:v7IfmfZm5gPDzg==@localhost:5672/Staging-Mark1")
            };
            var queueName = "TestWodQueueWithArguments";

            HandlerConsumerNew(logger, factory, queueName);

            logger.Information("** Finaliznando o consumode de mensagens **");
        }

        public static void HandlerConsumerNew(Logger logger, ConnectionFactory factory, string queueName)
        {
            var exchangeName = "ProducerTestExchange2";
            var routingKey = "mensagem.*";

            using (var conn = factory.CreateConnection())
            {
                using (var channel = conn.CreateModel())
                {
                    var headers = new Dictionary<string, object>
                    {
                        { "x-match", "all" },
                        { "headerKey", Guid.NewGuid().ToString() },
                        {"Property2","Valor da property 3"},
                    };

                    channel.QueueBind(queueName, exchangeName, routingKey, arguments: headers);

                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var body = ea.Body.ToArray();
                        var message = Encoding.UTF8.GetString(body);
                        logger.Information($"Estou recebendo a mensagem: {message}");
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


