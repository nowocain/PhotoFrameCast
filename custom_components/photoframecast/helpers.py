import asyncio
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote
from aiohttp import web
from homeassistant.core import HomeAssistant
from homeassistant.components.http import HomeAssistantView
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.network import get_url
from homeassistant.components.media_player import MediaPlayerEntityFeature

_LOGGER = logging.getLogger(__name__)

async def notify_user(hass: HomeAssistant, title: str, message: str, notification_id: str = "photoframecast"):
    """Send a persistent notification to the Home Assistant UI."""
    from homeassistant.components.persistent_notification import async_create as create_notification
    create_notification(hass, message=message, title=title, notification_id=notification_id)

def collect_photos(folder: Path, recursive: bool, shuffle: bool, sort_folder_by_folder: bool, sort_order: str = "alpha"):
    """Collect photo paths efficiently with optional folder-by-folder sorting."""

    def sort_files(files):
        if shuffle:
            return files
        if sort_order == "alpha":
            return sorted(files, key=lambda x: x.name.lower())
        elif sort_order == "newest":
            return sorted(files, key=lambda x: x.stat().st_mtime, reverse=True)
        elif sort_order == "oldest":
            return sorted(files, key=lambda x: x.stat().st_mtime)
        else:
            return files

    photos = []
    if recursive:
        if sort_folder_by_folder:
            base_files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
            photos.extend(sort_files(base_files))
            for subfolder in sorted(folder.rglob("*")):
                if subfolder.is_dir() and subfolder != folder:
                    files = [f for f in subfolder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
                    photos.extend(sort_files(files))
        else:
            files = [f for f in folder.rglob("*") if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
            photos.extend(sort_files(files))
    else:
        files = [f for f in folder.iterdir() if f.is_file() and f.suffix.lower() in (".jpg", ".jpeg", ".png")]
        photos.extend(sort_files(files))

    return photos


async def wait_if_busy(hass, entity_id, urls, force, next_photo, start_time=None, max_runtime_seconds=None):
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

        if start_time and max_runtime_seconds:
            elapsed = (datetime.now() - start_time).total_seconds()
            if elapsed >= max_runtime_seconds:
                _LOGGER.info(
                    "PhotoFrameCast: Max runtime reached while waiting for device %s, cleaning up",
                    entity_id
                )
                return False

        if busy:
            if not busy_logged:
                _LOGGER.info(
                    "PhotoFrameCast: %s was busy with other media, checking every 20s...",
                    entity_id
                )
                busy_logged = True
            await asyncio.sleep(20)
        else:
            if busy_logged:
                try:
                    photo_name = Path(next_photo).name
                except Exception:
                    photo_name = str(next_photo)
                _LOGGER.info(
                    "PhotoFrameCast: %s is now free, resuming slideshow with %s",
                    entity_id, photo_name
                )
            return True


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
        _LOGGER.error(
            "PhotoFrameCast: Failed to cast %s on %s: %s", url, entity_id, msg
        )
        await notify_user(
            hass,
            "PhotoFrameCast Error",
            f"Failed to cast {url} on {entity_id}: {msg}",
        )

        # Mark as incompatible only on known error patterns
        if "No playable items found" in msg or "Invalid media type" in msg:
            _LOGGER.warning(
                "PhotoFrameCast: Slideshow on %s was aborted: incompatible media player or unsupported format",
                entity_id,
            )
            await notify_user(
                hass,
                "PhotoFrameCast Warning",
                f"Incompatible media player for {entity_id}; slideshow aborted",
            )
            # Instead of stopping here, raise specific error
            raise HomeAssistantError(
                f"Incompatible media player: {entity_id}"
            ) from e

        raise
