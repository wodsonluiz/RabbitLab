using RabbitMQ.Client;
using Serilog;
using Serilog.Core;
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

            var factory = new ConnectionFactory()
            {
                Uri = new Uri("amqp://mark1-api:v7IfmfZm5gPDzg==@localhost:5672/Staging-Mark1")
            };

            //PublicOrderInExchangeTypeDirect(logger: logger, factory: factory, exchangeName: "Mark1", routingKey: "orders.#");
            //PublicOrderInExchangeTypeDirect(logger: logger, factory: factory, exchangeName: "Mark1", routingKey: "charges.#");
            PublicOrderInExchangeTypeHeaders(logger: logger, factory: factory, exchangeName: "Mark1.Headers", routingKey: "orders.#");
        }
    
        static void PublicOrderInExchangeTypeDirect(Logger logger, ConnectionFactory factory, string exchangeName, string routingKey)
        {
            logger.Information($"** Publicando order na exchange do tipo DIRECT **");

            try
            {
                using (var conn = factory.CreateConnection())
                {
                    using (var channel = conn.CreateModel())
                    {
                        var message = Encoding.UTF8.GetBytes($"Mensagem teste {Guid.NewGuid()}");

                        channel.BasicPublish(exchange: exchangeName,
                                            routingKey: routingKey,
                                            basicProperties: null,
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
                logger.Information($"** Finalizando a publicação da mensagem **");
            }
        }
        
        static void PublicOrderInExchangeTypeHeaders(Logger logger, ConnectionFactory factory, string exchangeName, string routingKey)
        {
            logger.Information($"** Publicando order na exchange do tipo Headers **");

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
                            { "Transaction-Poi","Transaction-From-Mark1"},
                        };
                       
                        channel.BasicPublish(exchange: exchangeName,
                                            routingKey: routingKey,
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
                logger.Information($"** Finalizando a publicação da mensagem **");
            }
        }
    }
}


