using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using SiteQuestaoKafka.Kafka;

namespace SiteQuestaoKafka.Controllers
{
    public class VotacaoController : Controller
    {
        private readonly ILogger<VotacaoController> _logger;
        private readonly VotacaoProducer _producer;

        public VotacaoController(ILogger<VotacaoController> logger,
            VotacaoProducer producer)
        {
            _logger = logger;
            _producer = producer;
        }

        public async Task<IActionResult> VotoAzureDevOps()
        {
            return await ProcessarVoto("Azure DevOps");
        }

        public async Task<IActionResult> VotoGitHubActions()
        {
            return await ProcessarVoto("GitHub Actions");
        }

        public async Task<IActionResult> VotoTerraform()
        {
            return await ProcessarVoto("Terraform");
        }

        public async Task<IActionResult> VotoKubernetes()
        {
            return await ProcessarVoto("Kubernetes");
        }

        private async Task<IActionResult> ProcessarVoto(string tecnologia)
        {
            _logger.LogInformation($"Processando voto para a tecnologia: {tecnologia}");
            await _producer.Send(tecnologia);
            _logger.LogInformation($"Informações sobre o voto '{tecnologia}' enviadas para o Apache Kafka!");

            TempData["Voto"] = tecnologia;
            return RedirectToAction("Index", "Home");
        }
    }
}