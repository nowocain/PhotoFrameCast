import asyncio
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path
from urllib.parse import quote

from aiohttp import web
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.typing import ConfigType
from homeassistant.components.http import HomeAssistantView
from homeassistant.helpers.network import get_url
from homeassistant.helpers.integration_platform import async_process_integration_platforms
from homeassistant.components.media_player import MediaPlayerEntityFeature
from homeassistant.helpers.storage import Store
import voluptuous as vol
from homeassistant.helpers import config_validation as cv

DOMAIN = "photoframecast"
STORAGE_KEY = f"{DOMAIN}_resume_data"
STORAGE_VERSION = 1
CONFIG_SCHEMA = cv.empty_config_schema
# Keep track of running slideshow or Photo of the Day tasks per device
running_tasks: dict[str, asyncio.Task] = {}  # keyed by entity_id
_LOGGER = logging.getLogger(__name__)

# ----------------- Helper Functions ----------------- #
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
                _LOGGER.info(
                    "PhotoFrameCast: %s was busy with other media, checking every 20s...",
                    entity_id,
                )
                busy_logged = True
            await asyncio.sleep(20)
        else:
            if busy_logged:
                _LOGGER.info(
                    "PhotoFrameCast: %s is now free, resuming slideshow with %s",
                    entity_id,
                    Path(next_photo).name,
                )
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
        _LOGGER.error(
            "PhotoFrameCast: Failed to cast %s on %s: %s", url, entity_id, msg
        )
        await notify_user(hass, "PhotoFrameCast Error", f"Failed to cast {url} on {entity_id}: {msg}")
        if "No playable items found" in msg or "Invalid media type" in msg:
            _LOGGER.warning(
                "PhotoFrameCast: Slideshow on %s was aborted: incompatible media player or unsupported format",
                entity_id,
            )
            await notify_user(hass, "PhotoFrameCast Warning", f"Incompatible media player for {entity_id}; slideshow aborted")
            await stop_slideshow(hass, entity_id, turn_off=False)
            raise RuntimeError("Incompatible media player, slideshow aborted")

async def run_slideshow(
    hass, entity_id, urls, interval, loop_forever, shuffle, force, max_runtime_seconds, resume
):
    """Run slideshow for a media player, respecting busy state, resume, and force."""

    start_time = datetime.now()

    # --- Determine starting index ---
    start_index = 0
    if resume and not shuffle:
        start_index = hass.data[DOMAIN]["resume_data"].get(entity_id, 0)

    if shuffle:
        random.shuffle(urls)
        start_index = 0

    if start_index > 0 and resume and not shuffle:
        _LOGGER.info(
            "PhotoFrameCast: Slideshow on %s resumed from photo #%d",
            entity_id, start_index + 1
        )
    else:
        _LOGGER.info(
            "PhotoFrameCast: Slideshow on %s started from the beginning",
            entity_id
        )

    # --- Wait initially if busy and force=False ---
    if not force:
        await wait_if_busy(hass, entity_id, urls, force, urls[start_index])

    try:
        index = start_index
        while True:
            # --- Loop end handling ---
            if index >= len(urls):
                if not loop_forever:
                    _LOGGER.info(
                        "PhotoFrameCast: Slideshow on %s completed; all photos were displayed once",
                        entity_id
                    )
                    await stop_slideshow(hass, entity_id, turn_off=True)

                    # Only clear resume_data if slideshow naturally completed
                    hass.data[DOMAIN]["resume_data"].pop(entity_id, None)
                    await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
                    return
                index = 0

            # --- Max runtime check ---
            elapsed = (datetime.now() - start_time).total_seconds()
            if max_runtime_seconds and max_runtime_seconds > 0 and elapsed >= max_runtime_seconds:
                elapsed_str = str(timedelta(seconds=int(elapsed)))
                _LOGGER.info(
                    "PhotoFrameCast: Maximum runtime reached on %s (%s elapsed); slideshow was stopped",
                    entity_id, elapsed_str
                )
                await stop_slideshow(hass, entity_id, turn_off=True)
                return

            # --- Wait if busy for long slideshows ---
            if not force:
                await wait_if_busy(hass, entity_id, urls, force, urls[index])

            # --- Play current photo ---
            try:
                await play_photo(hass, entity_id, urls[index], interval)
                # Save progress for resume
                hass.data[DOMAIN]["resume_data"][entity_id] = index
                await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            except RuntimeError:
                return

            index += 1

    except asyncio.CancelledError:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s was cancelled", entity_id)

    except Exception as e:
        _LOGGER.error(
            "PhotoFrameCast: Unexpected error during slideshow on %s: %s", entity_id, e
        )
        await notify_user(hass, "PhotoFrameCast Error", f"Unexpected error on {entity_id}: {e}")

    finally:
        await asyncio.sleep(0)
        # --- Cleanup running_slideshows ---
        hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        # --- Cleanup global running_tasks ---
        if entity_id in running_tasks and running_tasks[entity_id] == asyncio.current_task():
            del running_tasks[entity_id]

        _LOGGER.info("PhotoFrameCast: Slideshow resources were cleaned for %s", entity_id)

async def stop_slideshow(hass, entity_id, turn_off=True):
    """Stop an active slideshow or photo of the day safely."""

    # --- Cancel any scheduled photo-of-the-day timers ---
    timers = hass.data.setdefault(DOMAIN, {}).setdefault("photo_timers", {})
    if entity_id in timers:
        handle = timers.pop(entity_id)
        handle.cancel()
        _LOGGER.debug("PhotoFrameCast: canceled scheduled photo-of-the-day stop for %s", entity_id)

    # --- Cancel slideshow task if running ---
    slideshow = hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
    if slideshow:
        task = slideshow.get("task")
        if task:
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                _LOGGER.info("PhotoFrameCast: Slideshow task for %s was cancelled", entity_id)
            except Exception as e:
                _LOGGER.error(
                    "PhotoFrameCast: Error while cancelling slideshow task for %s: %s",
                    entity_id,
                    e,
                )
                await notify_user(
                    hass,
                    "PhotoFrameCast Error",
                    f"Error cancelling slideshow task for {entity_id}: {e}",
                )

    # --- Stop media player session if session is active ---
    state = hass.states.get(entity_id)
    supported = state.attributes.get("supported_features", 0) if state else 0
    media_status = state.attributes.get("media_content_id") if state else None

    if media_status:
        try:
            await hass.services.async_call(
                "media_player", "media_stop", {"entity_id": entity_id}, blocking=True
            )
        except Exception:
            _LOGGER.debug("PhotoFrameCast: media_stop failed, ignoring for %s", entity_id)

    # --- Optionally turn off the player ---
    if turn_off:
        if supported & MediaPlayerEntityFeature.TURN_OFF:
            try:
                await hass.services.async_call(
                    "media_player",
                    "turn_off",
                    {"entity_id": entity_id},
                    blocking=False,
                )
                _LOGGER.info("PhotoFrameCast: Media player %s was turned off", entity_id)
            except Exception as e:
                _LOGGER.error(
                    "PhotoFrameCast: Failed to turn off media player %s: %s", entity_id, e
                )
                await notify_user(
                    hass,
                    "PhotoFrameCast Error",
                    f"Failed to turn off media player {entity_id}: {e}",
                )
        else:
            _LOGGER.info(
                "PhotoFrameCast: Media player %s does not support turn_off, skipped",
                entity_id,
            )

# ----------------- Service Handlers ----------------- #

async def start_slideshow_service(call: ServiceCall):
    entity_id = call.data.get("entity_id")
    if not entity_id:
        _LOGGER.warning(
            "PhotoFrameCast: No media player was specified; slideshow was not started"
        )
        await notify_user(call.hass, "PhotoFrameCast Warning", "No media player specified; slideshow not started")
        return

    hass = call.hass
    force = call.data.get("force", True)

    # --- Check existing slideshow for this entity ---
    running = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
    if running:
        if not force:
            _LOGGER.info(
                "PhotoFrameCast: A slideshow is already running on %s, but force=False; new slideshow will not start",
                entity_id,
            )
            await notify_user(call.hass,"PhotoFrameCast Warning", f"A slideshow is already running on {entity_id}, but force=False; new slideshow will not start",
            )
            return
        else:
            _LOGGER.info(
                "PhotoFrameCast: A previous slideshow was running on %s and will be cancelled to start the new one",
                entity_id,
            )
            await stop_slideshow(hass, entity_id, turn_off=False)

    folder_path = Path(call.data.get("folder"))
    interval = int(call.data.get("interval", 5))
    shuffle = call.data.get("shuffle", True)
    recursive = call.data.get("recursive", True)
    loop_forever = call.data.get("loop", True)
    force = call.data.get("force", True)
    max_runtime_minutes = int(call.data.get("max_runtime", 720))
    sort_folder_by_folder = call.data.get("sort_folder_by_folder", True)
    resume = call.data.get("resume", True)
    max_runtime_seconds = max_runtime_minutes * 60 if max_runtime_minutes > 0 else 0

    state = hass.states.get(entity_id)
    if not state:
        _LOGGER.error("PhotoFrameCast: Media player %s was not found.", entity_id)
        await notify_user(hass, "PhotoFrameCast Error", f"Media player {entity_id} not found")
        return

    supported = state.attributes.get("supported_features", 0)
    if not supported & MediaPlayerEntityFeature.PLAY_MEDIA:
        _LOGGER.warning(
            "PhotoFrameCast: Media player %s does not support play_media; slideshow was not started",
            entity_id,
        )
        await notify_user(hass, "PhotoFrameCast Warning", f"Media player {entity_id} does not support play_media")
        return

    if not folder_path.is_dir():
        _LOGGER.error("PhotoFrameCast: Folder does not exist: %s.", folder_path)
        await notify_user(hass, "PhotoFrameCast Error", f"Folder does not exist: {folder_path}")
        return

    photos = await hass.async_add_executor_job(
        collect_photos, folder_path, recursive, shuffle, sort_folder_by_folder
    )
    if not photos:
        _LOGGER.warning("PhotoFrameCast: No photos were found in folder %s", folder_path)
        await notify_user(hass, "PhotoFrameCast Warning", f"No photos found in folder {folder_path}")
        return

    base_url = get_url(hass, prefer_external=True)
    urls = [
        f"{base_url}/api/photoframecast/{entity_id}/{quote(str(p).replace('\\', '/'))}"
        for p in photos
        if (folder_path / p).is_file()
    ]
    if not urls:
        _LOGGER.warning(
            "PhotoFrameCast: No valid photos were found to cast for %s", entity_id
        )
        await notify_user(hass, "PhotoFrameCast Warning", f"No valid photos found to cast for {entity_id}")
        return

    runtime_str = "unlimited" if max_runtime_minutes == 0 else \
        f"{max_runtime_minutes}m" if max_runtime_minutes < 60 else \
        f"{max_runtime_minutes//60}h" + (f"{max_runtime_minutes%60}m" if max_runtime_minutes%60 else "")

    _LOGGER.info(
        "PhotoFrameCast: Slideshow started on %s (%d photos, interval=%ds, max_runtime=%s, shuffle=%s, sort_folder_by_folder=%s, resume=%s, recursive=%s, loop=%s, force=%s)",
        entity_id,
        len(urls),
        interval,
        runtime_str,
        shuffle,
        sort_folder_by_folder,
        resume,
        recursive,
        loop_forever,
        force,
    )

    # Only stop previous slideshow if force=True
    if entity_id in hass.data[DOMAIN]["running_slideshows"]:
        if force:
            _LOGGER.info(
                "PhotoFrameCast: A previous slideshow was running on %s and has been cancelled to start the new one",
                entity_id,
            )
            await stop_slideshow(hass, entity_id, turn_off=False)
        else:
            _LOGGER.info(
                "PhotoFrameCast: A slideshow is already running on %s, but force=False; ",
                entity_id,
            )

    # Start slideshow task
    task = asyncio.create_task(
        run_slideshow(
            hass,
            entity_id,
            urls,
            interval,
            loop_forever,
            shuffle,
            force,
            max_runtime_seconds,
            resume,
        )
    )

    hass.data[DOMAIN]["running_slideshows"][entity_id] = {
        "task": task,
        "folder": folder_path,
        "urls": urls,
        "max_runtime": max_runtime_seconds,
    }

async def stop_slideshow_service(call: ServiceCall):
    hass = call.hass
    entity_id = call.data.get("entity_id")  # <- this was missing or misreferenced
    turn_off = call.data.get("turn_off", True)

    if not entity_id:
        _LOGGER.warning(
            "PhotoFrameCast: No media player was specified; slideshow could not be stopped"
        )
        await notify_user(call.hass, "PhotoFrameCast Warning", "No media player specified; slideshow could not be stopped")
        return
    turn_off = call.data.get("turn_off", True)
    await stop_slideshow(call.hass, entity_id, turn_off=turn_off)


async def reset_resume_service(call: ServiceCall):
    entity_id = call.data.get("entity_id")
    hass = call.hass

    if entity_id:
        if entity_id in hass.data[DOMAIN]["resume_data"]:
            hass.data[DOMAIN]["resume_data"].pop(entity_id)
            await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            _LOGGER.info(
                "PhotoFrameCast: Resume index was reset for %s",
                entity_id,
            )
            await notify_user(hass, "PhotoFrameCast Info", f"Resume index reset for {entity_id}")
        else:
            _LOGGER.info(
                "PhotoFrameCast: No resume index was reset for %s",
                entity_id,
            )
    else:
        if hass.data[DOMAIN]["resume_data"]:
            reset_entities = list(hass.data[DOMAIN]["resume_data"].keys())
            hass.data[DOMAIN]["resume_data"].clear()
            await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            _LOGGER.info(
                "PhotoFrameCast: Resume indexes were reset for all devices: %s",
                reset_entities,
            )
            await notify_user(hass, "PhotoFrameCast Info", f"Resume indexes reset for all devices: {reset_entities}")
        else:
            _LOGGER.info("PhotoFrameCast: No resume indexes were reset")

# ----------------- Photo of the Day Service ----------------- #

async def photo_of_the_day_service(call: ServiceCall):
    """Show one random photo, integrated with run_slideshow for proper logging and max_runtime."""
    import random
    from urllib.parse import quote

    hass = call.hass
    entity_id = call.data.get("entity_id")
    folder_path = Path(call.data.get("folder"))
    max_runtime_seconds = int(call.data.get("max_runtime", 60))  # <- now in seconds
    recursive = call.data.get("recursive", True)
    force = call.data.get("force", True)

    if not entity_id:
        await notify_user(
            hass,
            "PhotoFrameCast Warning",
            "No media player specified for Photo of the Day",
        )
        return

    if not folder_path.is_dir():
        await notify_user(
            hass,
            "PhotoFrameCast Error",
            f"Folder does not exist: {folder_path}",
        )
        return

    photos = await hass.async_add_executor_job(
        collect_photos, folder_path, recursive, True, True
    )
    if not photos:
        await notify_user(
            hass,
            "PhotoFrameCast Warning",
            f"No photos found in folder {folder_path}",
        )
        return

    # Pick one random photo
    photo = random.choice(photos)
    relative_path = quote(str(photo).replace("\\", "/"))
    base_url = get_url(hass, prefer_external=True)
    urls = [f"{base_url}/api/photoframecast/{entity_id}/{relative_path}"]

    # Ensure data structure exists
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("running_slideshows", {})

    if entity_id in hass.data[DOMAIN]["running_slideshows"]:
        existing_task_info = hass.data[DOMAIN]["running_slideshows"][entity_id]
        existing_task = existing_task_info["task"]
        if force:
            existing_task.cancel()
            _LOGGER.info(
                "PhotoFrameCast: A previous task was running on %s and has been cancelled to start Photo of the Day",
                entity_id,
            )
            try:
                # Wait until the cancelled task finishes cleanup
                await existing_task
            except asyncio.CancelledError:
                pass
            hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        else:
            _LOGGER.warning(
                "PhotoFrameCast: Photo of the Day requested for %s but a slideshow is already running, but force=False; Photo of the Day will not start",
                entity_id,
            )
            await notify_user(
                call.hass,
                "PhotoFrameCast Warning",
                f"A slideshow is already running on {entity_id}, but force=False; Photo of the Day will not start",
            )
            return

    _LOGGER.info(
        "PhotoFrameCast: Photo of the Day started on %s (%s, max_runtime=%ds, force=%s)",
        entity_id,
        photo.name,
        max_runtime_seconds,
        force,
    )

    # Start a mini-slideshow task for one photo (via HTTPView)
    task = asyncio.create_task(
        run_slideshow(
            hass,
            entity_id,
            urls,
            interval=max_runtime_seconds,  # show once for full duration
            loop_forever=False,
            shuffle=False,
            force=force,
            max_runtime_seconds=max_runtime_seconds,
            resume=False,
        )
    )

    hass.data[DOMAIN]["running_slideshows"][entity_id] = {
        "task": task,
        "folder": folder_path,
        "urls": urls,
        "max_runtime": max_runtime_seconds,
    }

# ----------------- HTTP View ----------------- #
class GlobalPhotoView(HomeAssistantView):
    requires_auth = False
    url = "/api/photoframecast/{entity_id}/{filename:.*}"
    name = "api:photoframecast"

    def __init__(self, hass: HomeAssistant):
        self.hass = hass

    async def get(self, request, entity_id, filename):
        slideshow = self.hass.data[DOMAIN]["running_slideshows"].get(entity_id)
        if not slideshow:
            return web.Response(status=404, text="No active slideshow")

        folder_path: Path = slideshow["folder"]
        file_path = folder_path / filename

        try:
            if not file_path.resolve().is_relative_to(folder_path.resolve()):
                return web.Response(status=403, text="Forbidden")
        except Exception:
            return web.Response(status=403, text="Forbidden")

        if not await self.hass.async_add_executor_job(file_path.is_file):
            return web.Response(status=404, text="File not found")

        return web.FileResponse(file_path)

# ----------------- Setup ----------------- #
async def async_setup(hass: HomeAssistant, config: ConfigType):
    """Set up the PhotoFrameCast integration asynchronously."""
    await async_process_integration_platforms(hass, DOMAIN, "services")

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("running_slideshows", {})

    # Persistent storage for resume feature
    hass.data[DOMAIN]["store"] = Store(hass, STORAGE_VERSION, STORAGE_KEY)
    stored = await hass.data[DOMAIN]["store"].async_load()
    if stored is None:
        stored = {}
    hass.data[DOMAIN]["resume_data"] = stored

    hass.http.register_view(GlobalPhotoView(hass))

    # Register Services
    hass.services.async_register(DOMAIN, "start_slideshow", start_slideshow_service)
    hass.services.async_register(DOMAIN, "stop_slideshow", stop_slideshow_service)
    hass.services.async_register(DOMAIN, "reset_resume", reset_resume_service)
    hass.services.async_register(DOMAIN, "photo_of_the_day", photo_of_the_day_service)

    return True
