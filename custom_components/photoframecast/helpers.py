import asyncio
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from aiohttp import web
from homeassistant.core import HomeAssistant
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers.network import get_url
from homeassistant.components.media_player import MediaPlayerEntityFeature

_LOGGER = logging.getLogger(__name__)

async def notify_user(hass: HomeAssistant, title: str, message: str, notification_id: str = "photoframecast"):
    """Send a persistent notification to the Home Assistant UI."""
    from homeassistant.components.persistent_notification import async_create as create_notification
    create_notification(hass, message=message, title=title, notification_id=notification_id)

def collect_photos(folder: Path, recursive: bool, shuffle: bool, sort_folder_by_folder: bool):
    """Collect photo paths efficiently with optional folder-by-folder sorting."""
    photos = []
    if recursive:
        if sort_folder_by_folder:
            base_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
            if not shuffle:
                base_files.sort(key=lambda x: x.name.lower())
            photos.extend(f.relative_to(folder) for f in base_files)
            for subfolder in sorted(folder.rglob("*")):
                if subfolder.is_dir() and subfolder != folder:
                    files = [f for f in subfolder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
                    if not shuffle:
                        files.sort(key=lambda x: x.name.lower())
                    photos.extend(f.relative_to(folder) for f in files)
        else:
            files = [f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
            if not shuffle:
                files.sort(key=lambda x: x.name.lower())
            photos.extend(f.relative_to(folder) for f in files)
    else:
        files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
        if not shuffle:
            files.sort(key=lambda x: x.name.lower())
        photos.extend(f.relative_to(folder) for f in files)
    return photos

async def wait_if_busy(hass, entity_id, urls, force, next_photo):
    """Wait until the media player is not busy playing other media, logging periodically."""
    busy_logged = False
    while True:
        state = hass.states.get(entity_id)
        media_id = state.attributes.get("media_content_id") if state else None
        media_type = state.attributes.get("media_content_type") if state else None
        position = state.attributes.get("media_position", 0)
        duration = state.attributes.get("media_duration", 0)
        busy = (
            not force
            and media_id
            and media_id not in urls
            and media_type in ["video", "music"]
            and not (position == 0 and duration == 0)
        )
        if busy:
            if not busy_logged:
                _LOGGER.info("PhotoFrameCast: %s was busy with other media, checking every 20s...", entity_id)
                busy_logged = True
            await asyncio.sleep(20)
        else:
            if busy_logged:
                _LOGGER.info("PhotoFrameCast: %s is now free, resuming slideshow with %s", entity_id, Path(next_photo).name)
            break

async def play_photo(hass, entity_id, url, interval):
    """Play a single photo on the media player."""
    try:
        await asyncio.shield(
            hass.services.async_call(
                "media_player",
                "play_media",
                {
                    "entity_id": entity_id,
                    "media_content_type": "image/jpeg",
                    "media_content_id": url,
                },
                blocking=True,
            )
        )
        if interval > 0:
            await asyncio.shield(asyncio.sleep(interval))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        msg = str(e)
        _LOGGER.error("PhotoFrameCast: Failed to cast %s on %s: %s", url, entity_id, msg)
        await notify_user(hass, "PhotoFrameCast Error", f"Failed to cast {url} on {entity_id}: {msg}")
        if "No playable items found" in msg or "Invalid media type" in msg:
            _LOGGER.warning("PhotoFrameCast: Slideshow on %s was aborted: incompatible media player or unsupported format", entity_id)
            await notify_user(hass, "PhotoFrameCast Warning", f"Incompatible media player for {entity_id}; slideshow aborted")
            await stop_slideshow(hass, entity_id, turn_off=False)
            raise RuntimeError("Incompatible media player, slideshow aborted")
