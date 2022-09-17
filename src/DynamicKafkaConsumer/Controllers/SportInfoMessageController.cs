namespace DynamicKafkaConsumer.Controllers;

using Microsoft.AspNetCore.Mvc;
using Models;
using Producers;

[ApiController]
[Route("[controller]")]
public class SportInfoMessageController : ControllerBase
{
    private readonly BasicSportInfoMessageProducer _kafkaProducer;
    private readonly ILogger<SportInfoMessageController> _logger;

    public SportInfoMessageController(BasicSportInfoMessageProducer kafkaProducer, ILogger<SportInfoMessageController> logger)
    {
        _kafkaProducer = kafkaProducer;
        _logger = logger;
    }

    /// <summary>
    /// Publishes sportInfoMessage
    /// </summary>
    /// <param name="keysToConfigure">KafkaKeys which the DynamicConsumer would process.</param>
    [HttpPost(Name = nameof(PublishSportInfoMessage))]
    [ProducesResponseType(typeof(IActionResult), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(IActionResult), StatusCodes.Status500InternalServerError)]
    public IActionResult PublishSportInfoMessage([FromBody] SportInfoMessageToPublish messageToPublish)
    {
        try
        {
            _kafkaProducer.Produce(messageToPublish.MessageKey, messageToPublish.Message);
            return Ok();
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed to publish message to kafka");
            return StatusCode(500, "An error occured during request. Please contact owners to further investigate.");
        }
    }
}