import asyncio
import logging
import random
from pathlib import Path
from urllib.parse import quote
from datetime import datetime, timedelta

from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.network import get_url
from homeassistant.components.media_player import MediaPlayerEntityFeature
from .helpers import notify_user, collect_photos, wait_if_busy, play_photo
from .const import DOMAIN, running_tasks

_LOGGER = logging.getLogger(__name__)

async def stop_slideshow(hass, entity_id, turn_off=True):
    """Stop an active slideshow or photo of the day safely."""
    timers = hass.data.setdefault(DOMAIN, {}).setdefault("photo_timers", {})
    if entity_id in timers:
        handle = timers.pop(entity_id)
        handle.cancel()
        _LOGGER.debug("PhotoFrameCast: canceled scheduled photo-of-the-day stop for %s", entity_id)
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
                _LOGGER.error("PhotoFrameCast: Error while cancelling slideshow task for %s: %s", entity_id, e)
                await notify_user(hass, "PhotoFrameCast Error", f"Error cancelling slideshow task for {entity_id}: {e}")
    state = hass.states.get(entity_id)
    supported = state.attributes.get("supported_features", 0) if state else 0
    media_status = state.attributes.get("media_content_id") if state else None
    if media_status:
        try:
            await hass.services.async_call("media_player", "media_stop", {"entity_id": entity_id}, blocking=True)
        except Exception:
            _LOGGER.debug("PhotoFrameCast: media_stop failed, ignoring for %s", entity_id)
    if turn_off:
        if supported & MediaPlayerEntityFeature.TURN_OFF:
            try:
                await hass.services.async_call("media_player", "turn_off", {"entity_id": entity_id}, blocking=False)
                _LOGGER.info("PhotoFrameCast: Media player %s was turned off", entity_id)
            except Exception as e:
                _LOGGER.error("PhotoFrameCast: Failed to turn off media player %s: %s", entity_id, e)
                await notify_user(hass, "PhotoFrameCast Error", f"Failed to turn off media player {entity_id}: {e}")
        else:
            _LOGGER.info("PhotoFrameCast: Media player %s does not support turn_off, skipped", entity_id)

async def run_slideshow(hass, entity_id, urls, interval, loop_forever, shuffle, force, max_runtime_seconds, resume):
    """Run slideshow for a media player, respecting busy state, resume, and force."""
    start_time = datetime.now()
    start_index = 0
    if resume and not shuffle:
        start_index = hass.data[DOMAIN]["resume_data"].get(entity_id, 0)
    if shuffle:
        random.shuffle(urls)
        start_index = 0
    if start_index > 0 and resume and not shuffle:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s resumed from photo #%d", entity_id, start_index + 1)
    else:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s started from the beginning", entity_id)
    if not force:
        await wait_if_busy(hass, entity_id, urls, force, urls[start_index])
    try:
        index = start_index
        while True:
            if index >= len(urls):
                if not loop_forever:
                    _LOGGER.info("PhotoFrameCast: Slideshow on %s completed; all photos were displayed once", entity_id)
                    await stop_slideshow(hass, entity_id, turn_off=True)
                    hass.data[DOMAIN]["resume_data"].pop(entity_id, None)
                    await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
                    return
                index = 0
            elapsed = (datetime.now() - start_time).total_seconds()
            if max_runtime_seconds and max_runtime_seconds > 0 and elapsed >= max_runtime_seconds:
                elapsed_str = str(timedelta(seconds=int(elapsed)))
                _LOGGER.info("PhotoFrameCast: Maximum runtime reached on %s (%s elapsed); slideshow was stopped", entity_id, elapsed_str)
                await stop_slideshow(hass, entity_id, turn_off=True)
                return
            if not force:
                await wait_if_busy(hass, entity_id, urls, force, urls[index])
            try:
                await play_photo(hass, entity_id, urls[index], interval)
                hass.data[DOMAIN]["resume_data"][entity_id] = index
                await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            except RuntimeError:
                return
            index += 1
    except asyncio.CancelledError:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s was cancelled", entity_id)
    except Exception as e:
        _LOGGER.error("PhotoFrameCast: Unexpected error during slideshow on %s: %s", entity_id, e)
        await notify_user(hass, "PhotoFrameCast Error", f"Unexpected error on {entity_id}: {e}")
    finally:
        await asyncio.sleep(0)
        hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        if entity_id in running_tasks and running_tasks[entity_id] == asyncio.current_task():
            del running_tasks[entity_id]
        _LOGGER.info("PhotoFrameCast: Slideshow resources were cleaned for %s", entity_id)

async def start_slideshow_service(call: ServiceCall):
    """Handle the start slideshow service call."""
    # ... (service handler logic remains here, but import and use helper functions)
    entity_id = call.data.get("entity_id")
    if not entity_id:
        _LOGGER.warning("PhotoFrameCast: No media player was specified; slideshow was not started")
        await notify_user(call.hass, "PhotoFrameCast Warning", "No media player specified; slideshow not started")
        return
    hass = call.hass
    force = call.data.get("force", True)
    running = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
    if running:
        if not force:
            _LOGGER.info("PhotoFrameCast: A slideshow is already running on %s, but force=False; new slideshow will not start", entity_id)
            await notify_user(call.hass,"PhotoFrameCast Warning", f"A slideshow is already running on {entity_id}, but force=False; new slideshow will not start")
            return
        else:
            _LOGGER.info("PhotoFrameCast: A previous slideshow was running on %s and will be cancelled to start the new one", entity_id)
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
        _LOGGER.warning("PhotoFrameCast: Media player %s does not support play_media; slideshow was not started", entity_id)
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
        _LOGGER.warning("PhotoFrameCast: No valid photos were found to cast for %s", entity_id)
        await notify_user(hass, "PhotoFrameCast Warning", f"No valid photos found to cast for {entity_id}")
        return
    runtime_str = "unlimited" if max_runtime_minutes == 0 else \
        f"{max_runtime_minutes}m" if max_runtime_minutes < 60 else \
        f"{max_runtime_minutes//60}h" + (f"{max_runtime_minutes%60}m" if max_runtime_minutes%60 else "")
    _LOGGER.info("PhotoFrameCast: Slideshow started on %s (%d photos, interval=%ds, max_runtime=%s, shuffle=%s, sort_folder_by_folder=%s, resume=%s, recursive=%s, loop=%s, force=%s)",
        entity_id, len(urls), interval, runtime_str, shuffle, sort_folder_by_folder, resume, recursive, loop_forever, force)
    if entity_id in hass.data[DOMAIN]["running_slideshows"]:
        if force:
            _LOGGER.info("PhotoFrameCast: A previous slideshow was running on %s and has been cancelled to start the new one", entity_id)
            await stop_slideshow(hass, entity_id, turn_off=False)
        else:
            _LOGGER.info("PhotoFrameCast: A slideshow is already running on %s, but force=False; ", entity_id)
    task = asyncio.create_task(
        run_slideshow(
            hass, entity_id, urls, interval, loop_forever, shuffle, force, max_runtime_seconds, resume)
    )
    hass.data[DOMAIN]["running_slideshows"][entity_id] = {
        "task": task, "folder": folder_path, "urls": urls, "max_runtime": max_runtime_seconds
    }

async def stop_slideshow_service(call: ServiceCall):
    """Handle the stop slideshow service call."""
    hass = call.hass
    entity_id = call.data.get("entity_id")
    turn_off = call.data.get("turn_off", True)
    if not entity_id:
        _LOGGER.warning("PhotoFrameCast: No media player was specified; slideshow could not be stopped")
        await notify_user(call.hass, "PhotoFrameCast Warning", "No media player specified; slideshow could not be stopped")
        return
    turn_off = call.data.get("turn_off", True)
    await stop_slideshow(call.hass, entity_id, turn_off=turn_off)

async def reset_resume_service(call: ServiceCall):
    """Handle the reset resume index service call."""
    entity_id = call.data.get("entity_id")
    hass = call.hass
    if entity_id:
        if entity_id in hass.data[DOMAIN]["resume_data"]:
            hass.data[DOMAIN]["resume_data"].pop(entity_id)
            await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            _LOGGER.info("PhotoFrameCast: Resume index was reset for %s", entity_id)
            await notify_user(hass, "PhotoFrameCast Info", f"Resume index reset for {entity_id}")
        else:
            _LOGGER.info("PhotoFrameCast: No resume index was reset for %s", entity_id)
    else:
        if hass.data[DOMAIN]["resume_data"]:
            reset_entities = list(hass.data[DOMAIN]["resume_data"].keys())
            hass.data[DOMAIN]["resume_data"].clear()
            await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
            _LOGGER.info("PhotoFrameCast: Resume indexes were reset for all devices: %s", reset_entities)
            await notify_user(hass, "PhotoFrameCast Info", f"Resume indexes reset for all devices: {reset_entities}")
        else:
            _LOGGER.info("PhotoFrameCast: No resume indexes were reset")

async def photo_of_the_day_service(call: ServiceCall):
    """Handle the photo of the day service call."""
    hass = call.hass
    entity_id = call.data.get("entity_id")
    folder_path = Path(call.data.get("folder"))
    max_runtime_seconds = int(call.data.get("max_runtime", 60))
    recursive = call.data.get("recursive", True)
    force = call.data.get("force", True)
    if not entity_id:
        await notify_user(hass, "PhotoFrameCast Warning", "No media player specified for Photo of the Day")
        return
    if not folder_path.is_dir():
        await notify_user(hass, "PhotoFrameCast Error", f"Folder does not exist: {folder_path}")
        return
    photos = await hass.async_add_executor_job(collect_photos, folder_path, recursive, True, True)
    if not photos:
        await notify_user(hass, "PhotoFrameCast Warning", f"No photos found in folder {folder_path}")
        return
    photo = random.choice(photos)
    relative_path = quote(str(photo).replace("\\", "/"))
    base_url = get_url(hass, prefer_external=True)
    urls = [f"{base_url}/api/photoframecast/{entity_id}/{relative_path}"]
    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN].setdefault("running_slideshows", {})
    if entity_id in hass.data[DOMAIN]["running_slideshows"]:
        existing_task_info = hass.data[DOMAIN]["running_slideshows"][entity_id]
        existing_task = existing_task_info["task"]
        if force:
            existing_task.cancel()
            _LOGGER.info("PhotoFrameCast: A previous task was running on %s and has been cancelled to start Photo of the Day", entity_id)
            try:
                await existing_task
            except asyncio.CancelledError:
                pass
            hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        else:
            _LOGGER.warning("PhotoFrameCast: Photo of the Day requested for %s but a slideshow is already running, but force=False; Photo of the Day will not start", entity_id)
            await notify_user(call.hass, "PhotoFrameCast Warning", f"A slideshow is already running on {entity_id}, but force=False; Photo of the Day will not start")
            return
    _LOGGER.info("PhotoFrameCast: Photo of the Day started on %s (%s, max_runtime=%ds, force=%s)", entity_id, photo.name, max_runtime_seconds, force)
    task = asyncio.create_task(
        run_slideshow(
            hass,
            entity_id,
            urls,
            interval=max_runtime_seconds,
            loop_forever=False,
            shuffle=False,
            force=force,
            max_runtime_seconds=max_runtime_seconds,
            resume=False,
        )
    )
    hass.data[DOMAIN]["running_slideshows"][entity_id] = {
        "task": task, "folder": folder_path, "urls": urls, "max_runtime": max_runtime_seconds
    }
