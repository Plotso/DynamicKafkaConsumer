namespace DynamicKafkaConsumer.Controllers;

using KafkaCommon.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;
using Models;

[ApiController]
[Route("[controller]")]
public class DynamicConsumerController : ControllerBase
{
    private readonly IDynamicConsumerModifier<string> _dynamicConsumerModifier;
    private readonly ILogger<DynamicConsumerController> _logger;

    public DynamicConsumerController(IDynamicConsumerModifier<string> dynamicConsumerModifier, ILogger<DynamicConsumerController> logger)
    {
        _dynamicConsumerModifier = dynamicConsumerModifier;
        _logger = logger;
    }

    /// <summary>
    /// Get current status of dynamic consumer via DynamicConsumerModifier
    /// </summary>
    [HttpGet(Name = "CurrentStatus")]
    [ProducesResponseType(typeof(DynamicConsumerStatus), StatusCodes.Status200OK)]
    public DynamicConsumerStatus Get()
    {
        var isRunning = _dynamicConsumerModifier.IsDynamicConsumerActive();
        var currentKeys = _dynamicConsumerModifier.GetKeysToProcess();
        return new DynamicConsumerStatus(isRunning, currentKeys);
    }

    /// <summary>
    /// Set keys that would trigger DynamicConsumer to process them until it reaches end of partition
    /// </summary>
    /// <param name="keysToConfigure">KafkaKeys which the DynamicConsumer would process.</param>
    [HttpPut(Name = nameof(SetKeys))]
    [ProducesResponseType(typeof(IActionResult), StatusCodes.Status200OK)]
    [ProducesResponseType(typeof(IActionResult), StatusCodes.Status409Conflict)]
    public IActionResult SetKeys(IEnumerable<string> keysToConfigure)
    {
        if (_dynamicConsumerModifier.IsDynamicConsumerActive())
            return Conflict("DynamicConsumer is currently processing previously configured batch of keys. When finished, it can be reconfigured again");
        try
        {
            _dynamicConsumerModifier.SetKeysToConsumer(keysToConfigure);
            _logger.LogInformation($"Successfully configured DynamicConsumer to process following keys: {string.Join(" ,", keysToConfigure)}");
            return Ok();
        }
        catch (Exception e)
        {
            _logger.LogError($"An error occured during {nameof(SetKeys)} request for keys: {string.Join(" ,", keysToConfigure)}");
            return StatusCode(500, "An error occured during request. Please contact owners to further investigate.");
        }
    }
}