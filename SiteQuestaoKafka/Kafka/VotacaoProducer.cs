using System;
using System.Text.Json;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Confluent.Kafka;

namespace SiteQuestaoKafka.Kafka
{
    public class VotacaoProducer
    {
        private readonly ILogger<VotacaoProducer> _logger;
        private readonly IConfiguration _configuration;
        private readonly JsonSerializerOptions _serializerOptions;

        public VotacaoProducer(
            ILogger<VotacaoProducer> logger,
            IConfiguration configuration)
        {
            _logger = logger;
            _configuration = configuration;
            _serializerOptions = new JsonSerializerOptions()
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase
            };
        }

        private async Task SendEventDataAsync<T>(IProducer<Null, string> producer, T eventData)
        {
            string topic = _configuration["ApacheKafka:Topic"];

            string data = JsonSerializer.Serialize(eventData, _serializerOptions);
            _logger.LogInformation($"Evento: {data}");

            var result = await producer.ProduceAsync(
                topic,
                new Message<Null, string>
                { Value = data });

            _logger.LogInformation(
                $"Apache Kafka - Envio para o tópico {topic} concluído | " +
                $"{data} | Status: { result.Status.ToString()}");
        }

        public async Task Send(string tecnologia)
        {
            var configKafka = new ProducerConfig
            {
                BootstrapServers = _configuration["ApacheKafka:Broker"],
                SecurityProtocol = SecurityProtocol.SaslSsl,
                SaslMechanism = SaslMechanism.Plain,
                SaslUsername = _configuration["ApacheKafka:Username"],
                SaslPassword = _configuration["ApacheKafka:Password"]
            };

            using (var producer = new ProducerBuilder<Null, string>(configKafka).Build())
            {
                var idVoto = Guid.NewGuid().ToString();
                var horario = $"{DateTime.UtcNow.AddHours(-3):yyyy-MM-dd HH:mm:ss}";

                await SendEventDataAsync<InstanciaVoto>(producer,
                    new ()
                    {
                        IdVoto = idVoto,
                        Horario = horario,
                        Instancia = Environment.MachineName
                    });

                await SendEventDataAsync<Voto>(producer,
                    new ()
                    {
                        IdVoto = idVoto,
                        Horario = horario,
                        Tecnologia = tecnologia
                    });
            }

            _logger.LogInformation("Concluido o envio dos eventos!");
        }
    }
}