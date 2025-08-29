import logging

_LOGGER = logging.getLogger(__name__)
DOMAIN = "photoframecast"

async def async_get_actions(hass, device_id=None):
    """Return available actions for the UI."""
    _LOGGER.debug("PhotoFrameCast: async_get_actions called")
    return [
        {
            "domain": DOMAIN,
            "type": "start_slideshow",
            "name": "Start Slideshow",
            "description": "Start a photo slideshow on a media player.",
            "fields": {
                "entity_id": {"name": "Media Player", "required": True},
                "folder": {"name": "Photo Folder", "required": True},
                "interval": {"name": "Interval (s)", "required": True, "default": 5},
                "shuffle": {"name": "Shuffle Photos", "required": False, "default": True},
                "sort_folder_by_folder": {
                    "name": "Folder-by-Folder Sorting",
                    "required": False,
                    "default": True,
                },
                "recursive": {"name": "Include Subfolders", "required": False, "default": True},
                "loop": {"name": "Loop Slideshow", "required": False, "default": True},
                "force": {"name": "Force Run", "required": False, "default": True},
                "max_runtime": {"name": "Max Runtime (minutes)", "required": False, "default": 720},
            },
        },
        {
            "domain": DOMAIN,
            "type": "stop_slideshow",
            "name": "Stop Slideshow",
            "description": "Stop the active photo slideshow on a media player.",
            "fields": {
                "entity_id": {"name": "Media Player", "required": True},
                "turn_off": {"name": "Turn Off Player", "required": False, "default": True},
            },
        },
    ]

async def async_call_action(hass, config, variables, context):
    """Execute the action."""
    _LOGGER.debug("PhotoFrameCast: async_call_action called for %s", config["type"])

    if config["type"] == "start_slideshow":
        await hass.helpers.service.async_call(
            DOMAIN,
            "start_slideshow",
            {
                "entity_id": config["entity_id"],
                "folder": config["folder"],
                "interval": config.get("interval", 5),
                "shuffle": config.get("shuffle", True),
                "sort_folder_by_folder": config.get("sort_folder_by_folder", True),
                "recursive": config.get("recursive", True),
                "loop": config.get("loop", True),
                "force": config.get("force", True),
                "max_runtime": config.get("max_runtime", 720),
            },
            context=context,
        )

    elif config["type"] == "stop_slideshow":
        entity_id = config.get("entity_id")
        if not entity_id:
            _LOGGER.warning("PhotoFrameCast: stop_slideshow called without entity_id")
            return

        await hass.helpers.service.async_call(
            DOMAIN,
            "stop_slideshow",
            {
                "entity_id": entity_id,
                "turn_off": config.get("turn_off", True),
            },
            context=context,
        )
