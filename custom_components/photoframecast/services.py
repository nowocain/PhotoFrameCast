import asyncio
import logging
import random
import voluptuous as vol
from pathlib import Path
from urllib.parse import quote
from datetime import datetime, timedelta
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.network import get_url
from homeassistant.exceptions import HomeAssistantError
from homeassistant.components.media_player import MediaPlayerEntityFeature
from homeassistant.helpers import config_validation as cv
from .helpers import notify_user, collect_photos, wait_if_busy, play_photo
from .const import DOMAIN, running_tasks

_LOGGER = logging.getLogger(__name__)

PAUSE_RESUME_SCHEMA = vol.Schema({vol.Optional("entity_id"): cv.string,})

SORT_ORDER_OPTIONS = ["alpha", "newest", "oldest"]

START_SLIDESHOW_SCHEMA = vol.Schema(
    {
        vol.Optional("entity_id"): cv.string,
        vol.Optional("folder"): cv.string,
        vol.Optional("shuffle", default=True): cv.boolean,
        vol.Optional("sort_order", default="alpha"): vol.In(SORT_ORDER_OPTIONS),
        vol.Optional("sort_folder_by_folder", default=True): cv.boolean,
        vol.Optional("interval", default={"seconds": 5}): dict,
        vol.Optional("loop", default=True): cv.boolean,
        vol.Optional("force", default=True): cv.boolean,
        vol.Optional("resume", default=True): cv.boolean,
        vol.Optional("recursive", default=True): cv.boolean,
        vol.Optional("max_runtime", default={"hours": 12}): dict,
        vol.Optional("sync_group", default=None): vol.Any(str, None),
    }
)

# -------------------------
# Utility converters
# -------------------------
def hms_to_seconds(hours: int = 0, minutes: int = 0, seconds: int = 0) -> int:
    """Convert hours/minutes/seconds to total seconds."""
    return hours * 3600 + minutes * 60 + seconds

def format_runtime(seconds: int) -> str:
    """Format runtime seconds into human-readable string."""
    if seconds <= 0:
        return "unlimited"
    td = timedelta(seconds=seconds)
    return str(td)

def parse_duration(duration) -> int:
    """Convert HA duration selector dict to total seconds."""
    if isinstance(duration, dict):
        hours = duration.get("hours", 0)
        minutes = duration.get("minutes", 0)
        seconds = duration.get("seconds", 0)
        return hours * 3600 + minutes * 60 + seconds
    if isinstance(duration, (int, float)):
        return int(duration)
    return 0
# -------------------------
# Sync tick updater
# -------------------------
async def sync_group_tick_updater(hass: HomeAssistant, sync_group: str, urls_count: int, interval: int):
    """Increment the tick for a sync group only when all devices are ready for next photo."""
    groups = hass.data[DOMAIN]["sync_groups"]
    group = groups[sync_group]

    group.setdefault("tick", 0)
    group.setdefault("devices_ready", set())
    group.setdefault("last_update", datetime.now())

    try:
        while True:
            await asyncio.sleep(0.1)  # small check interval

            devices_in_group = set(group.get("devices", []))
            if not devices_in_group:
                continue  # no devices yet

            # Wait until all devices are ready
            if group["devices_ready"] >= devices_in_group:
                # move to next tick
                group["tick"] = (group["tick"] + 1) % urls_count
                group["last_update"] = datetime.now()
                group["devices_ready"].clear()
                _LOGGER.debug(
                    "PhotoFrameCast: Sync group '%s' tick updated to %d",
                    sync_group, group["tick"]
                )
    except asyncio.CancelledError:
        _LOGGER.info("PhotoFrameCast: Sync group tick updater cancelled for '%s'", sync_group)
# -------------------------
# Stop slideshow
# -------------------------
async def stop_slideshow(hass, entity_id, turn_off=True):
    """Stop an active slideshow safely, including sync group cleanup."""
    # Cancel any scheduled photo timers
    timers = hass.data.setdefault(DOMAIN, {}).setdefault("photo_timers", {})
    if entity_id in timers:
        handle = timers.pop(entity_id)
        handle.cancel()
        _LOGGER.debug("PhotoFrameCast: canceled scheduled photo-of-the-day stop for %s", entity_id)

    # Cancel the running slideshow task
    slideshow = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
    task = None
    if slideshow:
        task = slideshow.get("task")
        if task and not task.done():
            try:
                task.cancel()
                await task
            except asyncio.CancelledError:
                _LOGGER.info("PhotoFrameCast: Slideshow task for %s was cancelled", entity_id)
            except Exception as e:
                _LOGGER.error(
                    "PhotoFrameCast: Error while cancelling slideshow task for %s: %s",
                    entity_id, e
                )
                await notify_user(hass, "PhotoFrameCast Error", f"Error cancelling slideshow task for {entity_id}: {e}")

    # Remove from running slideshows and running_tasks
    hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
    if entity_id in running_tasks and running_tasks[entity_id] == task:
        del running_tasks[entity_id]

    # Sync group cleanup if needed
    for group_name, group in hass.data.setdefault(DOMAIN, {}).setdefault("sync_groups", {}).items():
        if entity_id in group.get("devices", set()):
            group["devices"].remove(entity_id)
            group["devices_ready"].discard(entity_id)
            # If this was the owner, fully reset the group for a fresh next run
            if group.get("owner") == entity_id:
                group["tick"] = 0
                group["stopped"] = False
                if group.get("tick_task"):
                    group["tick_task"].cancel()
                    group["tick_task"] = None
                group["devices"] = set()
                group["devices_ready"] = set()
                _LOGGER.debug("PhotoFrameCast: Sync group '%s' fully reset for next run", group_name)

    # Stop media playback
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

    if turn_off and supported & MediaPlayerEntityFeature.TURN_OFF:
        try:
            await hass.services.async_call(
                "media_player", "turn_off", {"entity_id": entity_id}, blocking=False
            )
            _LOGGER.info("PhotoFrameCast: Media player %s was turned off", entity_id)
        except Exception as e:
            _LOGGER.error(
                "PhotoFrameCast: Failed to turn off media player %s: %s", entity_id, e
            )
            await notify_user(hass, "PhotoFrameCast Error", f"Failed to turn off media player {entity_id}: {e}")
    else:
        _LOGGER.info("PhotoFrameCast: Media player %s does not support turn_off, skipped", entity_id)

    _LOGGER.info("PhotoFrameCast: Slideshow resources cleaned for %s", entity_id)


# -------------------------
# Core slideshow loop
# -------------------------
async def run_slideshow(
    hass: HomeAssistant,
    entity_id: str,
    urls: list,
    interval: int,
    loop_forever: bool,
    shuffle: bool,
    force: bool,
    max_runtime_seconds: int,
    resume: bool,
    sync_group: str = None,
    start_index: int = 0,
):
    """Run slideshow for a media player, optionally synchronized in a group."""
    start_time = datetime.now()
    index = start_index

    _LOGGER.debug(
        "PhotoFrameCast: run_slideshow STARTED for %s, start_index=%d, sync_group=%s",
        entity_id, start_index, sync_group
    )

    group = None
    if sync_group:
        group = hass.data[DOMAIN]["sync_groups"][sync_group]

        # WATCHER: wait for owner to initialize urls and tick
        if entity_id != group.get("owner"):
            while "urls" not in group or "tick" not in group:
                _LOGGER.debug(
                    "PhotoFrameCast: watcher %s waiting for owner to initialize urls/tick", entity_id
                )
                await asyncio.sleep(0.1)
            # force first photo to owner's tick
            index = group["tick"]
            _LOGGER.debug(
                "PhotoFrameCast: watcher %s synced first index=%d with owner %s",
                entity_id, index, group.get("owner")
            )

    # Shuffle for owner or non-sync slideshows
    if shuffle and (not group or entity_id == group.get("owner")):
        random.shuffle(urls)
        _LOGGER.debug("PhotoFrameCast: shuffled urls for %s", entity_id)

    # Max runtime and pause handling but  watcher FORCE=TRUE FREEZED last IMAGE
    try:
        while True:
            current_index = index

            # --- MAX RUNTIME HANDLING ---
            if group:
                if entity_id == group.get("owner"):
                    elapsed = (datetime.now() - group["start_time"]).total_seconds()
                    if group.get("max_runtime", 0) > 0 and elapsed >= group["max_runtime"]:
                        if not group.get("stopped", False):
                            _LOGGER.info(
                                "PhotoFrameCast: Max runtime reached for sync group '%s', stopping all slideshows",
                                sync_group
                            )
                            group["stopped"] = True
                            for device in list(group.get("devices", set())):
                                task_info = hass.data[DOMAIN]["running_slideshows"].get(device)
                                if not task_info:
                                    continue
                                # Stop owner or force=True watchers
                                if device == group.get("owner") or task_info.get("force", False):
                                    await stop_slideshow(hass, device, turn_off=True)
                                else:
                                    # force=False watcher: leave media playing, just cleanup internal tracking
                                    _LOGGER.info(
                                        "PhotoFrameCast: Max runtime reached, leaving %s app open (force=False watcher)",
                                        device
                                    )
                                    hass.data[DOMAIN]["running_slideshows"].pop(device, None)
                        return
                else:
                    current_index = group["tick"]
            else:
                if max_runtime_seconds > 0:
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if elapsed >= max_runtime_seconds:
                        _LOGGER.info("PhotoFrameCast: Max runtime reached on %s, stopping", entity_id)
                        await stop_slideshow(hass, entity_id, turn_off=True)
                        return

            # --- PAUSE HANDLING ---
            if group:
                while True:
                    async with group["lock"]:
                        if not group.get("paused", False):
                            break
                    if group.get("max_runtime", 0) > 0:
                        elapsed = (datetime.now() - group["start_time"]).total_seconds()
                        if elapsed >= group["max_runtime"]:
                            _LOGGER.info(
                                "PhotoFrameCast: Max runtime reached while waiting for device in sync group '%s', cleaning up",
                                sync_group
                            )
                            group["stopped"] = True
                            for device in list(group.get("devices", set())):
                                task_info = hass.data[DOMAIN]["running_slideshows"].get(device)
                                if not task_info:
                                    continue
                                if device == group.get("owner") or task_info.get("force", False):
                                    await stop_slideshow(hass, device, turn_off=True)
                                else:
                                    _LOGGER.info(
                                        "PhotoFrameCast: Max runtime reached during pause, leaving %s app open (force=False watcher)",
                                        device
                                    )
                                    hass.data[DOMAIN]["running_slideshows"].pop(device, None)
                            return
                    await asyncio.sleep(0.1)
            else:
                task_info = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
                while task_info and task_info.get("paused", False):
                    elapsed = (datetime.now() - start_time).total_seconds()
                    if max_runtime_seconds > 0 and elapsed >= max_runtime_seconds:
                        _LOGGER.info(
                            "PhotoFrameCast: Max runtime reached while paused on %s, stopping slideshow",
                            entity_id
                        )
                        await stop_slideshow(hass, entity_id, turn_off=True)
                        return
                    await asyncio.sleep(0.5)

            # Watcher busy check
            if group and entity_id != group.get("owner"):
                state = hass.states.get(entity_id)
                media_id = state.attributes.get("media_content_id") if state else None
                media_type = state.attributes.get("media_content_type") if state else None
                position = state.attributes.get("media_position", 0)
                duration = state.attributes.get("media_duration", 0)

                busy = (
                    media_id
                    and media_id not in urls
                    and media_type in ["video", "music"]
                    and not (position == 0 and duration == 0)
                )

                if busy and not force:
                    _LOGGER.info(
                        "PhotoFrameCast: watcher %s is busy with other media, skipping this tick",
                        entity_id
                    )
                    if group:
                        group["devices_ready"].add(entity_id)
                        while group["tick"] == current_index and not group.get("stopped", False):
                            await asyncio.sleep(0.1)
                        index = group["tick"]
                    else:
                        index += 1
                    continue

            # --- FIXED WATCHER SYNC ---
            if group and entity_id != group.get("owner"):
                # WATCHER immediately follows owner's tick
                current_index = group["tick"]
                if force:
                    _LOGGER.debug(
                        "PhotoFrameCast: watcher %s syncing immediately to owner's tick=%d",
                        entity_id, current_index
                    )

            # Show photo
            if current_index < len(urls):
                _LOGGER.debug(
                    "PhotoFrameCast: Showing photo #%d/%d on %s (sync_group=%s, tick=%d)",
                    current_index + 1, len(urls), entity_id, sync_group, group["tick"] if group else -1
                )
                next_photo_url = urls[current_index]

                try:
                    if group and entity_id != group.get("owner"):
                        # WATCHER: always play owner's current tick
                        await play_photo(hass, entity_id, next_photo_url, interval)
                        hass.data[DOMAIN]["resume_data"][entity_id] = current_index
                        await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])
                    else:
                        # OWNER
                        can_play = await wait_if_busy(
                            hass, entity_id, urls, force,
                            next_photo=next_photo_url,
                            start_time=start_time,
                            max_runtime_seconds=max_runtime_seconds
                        )
                        if not can_play:
                            return
                        await play_photo(hass, entity_id, next_photo_url, interval)
                        hass.data[DOMAIN]["resume_data"][entity_id] = current_index
                        await hass.data[DOMAIN]["store"].async_save(hass.data[DOMAIN]["resume_data"])

                except HomeAssistantError as err:
                    _LOGGER.warning(str(err))
                    await stop_slideshow(hass, entity_id, turn_off=False)
                    return
                except Exception as err:
                    _LOGGER.error("PhotoFrameCast: Unexpected error in slideshow: %s", err)
                    await stop_slideshow(hass, entity_id, turn_off=False)
                    return

            # Tick handling
            if group:
                group["devices_ready"].add(entity_id)

                if entity_id != group.get("owner") and force:
                    # WATCHER: sync to owner's tick but advance once to avoid repeating
                    index = group["tick"] + 1
                    if index >= len(urls):
                        if loop_forever:
                            index = 0
                        else:
                            _LOGGER.info("PhotoFrameCast: Slideshow completed on %s", entity_id)
                            await stop_slideshow(hass, entity_id, turn_off=True)
                            return
                    # wait until owner moves forward to match index properly
                    while index != group["tick"]:
                        await asyncio.sleep(0.1)
                else:
                    # OWNER or non-forced watcher: normal slideshow
                    while group["tick"] == current_index and not group.get("stopped", False):
                        await asyncio.sleep(0.1)
                    index = group["tick"]
            else:
                # no group: normal slideshow
                index += 1
                if index >= len(urls) and loop_forever:
                    index = 0
                elif index >= len(urls) and not loop_forever:
                    _LOGGER.info("PhotoFrameCast: Slideshow completed on %s", entity_id)
                    await stop_slideshow(hass, entity_id, turn_off=True)
                    return

    except asyncio.CancelledError:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s was cancelled", entity_id)
    finally:
        hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        if group:
            group.get("devices", set()).discard(entity_id)
            group.get("devices_ready", set()).discard(entity_id)
        try:
            current = asyncio.current_task()
            if entity_id in running_tasks and running_tasks[entity_id] == current:
                del running_tasks[entity_id]
        except Exception:
            pass
        _LOGGER.info("PhotoFrameCast: Slideshow stopped on %s", entity_id)

# -------------------------
# Start slideshow service
# -------------------------
async def start_slideshow_service(call: ServiceCall):
    """Handle the start slideshow service call."""
    hass = call.hass
    entity_id = call.data.get("entity_id")
    if not entity_id:
        _LOGGER.warning(
            "PhotoFrameCast: No media player was specified; slideshow was not started"
        )
        await notify_user(
            hass, "PhotoFrameCast Warning", "No media player specified; slideshow not started"
        )
        return

    # Validate player exists and supports playback
    state = hass.states.get(entity_id)
    if not state:
        _LOGGER.warning("PhotoFrameCast: Entity %s not found", entity_id)
        await notify_user(hass, "PhotoFrameCast Error", f"Entity {entity_id} not found")
        return

    # Validate folder specified
    folder = call.data.get("folder")
    if not folder:
        _LOGGER.error("PhotoFrameCast: No folder specified for slideshow")
        await notify_user(hass, "PhotoFrameCast Error", "No folder specified for slideshow")
        return
    folder_path = Path(folder)

    supported = state.attributes.get("supported_features", 0)
    if not (supported & MediaPlayerEntityFeature.PLAY_MEDIA):
        _LOGGER.warning("PhotoFrameCast: %s does not support media playback", entity_id)
        await notify_user(hass, "PhotoFrameCast Error", f"{entity_id} does not support media playback")
        return

    force = call.data.get("force", True)

    # Stop existing slideshow if forced or check if it’s still running
    running = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
    if running:
        task = running.get("task")
        if task and not task.done():  # still running
            if not force:
                _LOGGER.info(
                    "PhotoFrameCast: A slideshow is already running on %s, but force=False; new slideshow will not start",
                    entity_id,
                )
                await notify_user(
                    hass, "PhotoFrameCast Warning",
                    f"A slideshow is already running on {entity_id}, but force=False; new slideshow will not start",
                )
                return
            else:
                _LOGGER.info(
                    "PhotoFrameCast: A previous slideshow was running on %s and will be cancelled to start the new one",
                    entity_id,
                )
                await stop_slideshow(hass, entity_id, turn_off=False)
                # If device was in a sync group, clean it up
                sync_group = running.get("sync_group") if running else None
                if sync_group:
                    group = hass.data[DOMAIN]["sync_groups"].get(sync_group)
                    if group:
                        # If owner is leaving, stop group completely
                        if group.get("owner") == entity_id:
                            _LOGGER.info(
                                "PhotoFrameCast: Owner %s left sync group %s, dissolving group",
                                entity_id, sync_group
                            )

                            # Cancel tick task
                            tick_task = group.get("tick_task")
                            if tick_task:
                                try:
                                    tick_task.cancel()
                                    await tick_task
                                except asyncio.CancelledError:
                                    pass
                                except Exception as e:
                                    _LOGGER.error(
                                        "PhotoFrameCast: Failed to cancel tick task for sync group %s: %s",
                                        sync_group, e
                                    )

                            # Stop all watchers
                            for watcher in list(group.get("devices", [])):
                                try:
                                    await stop_slideshow(hass, watcher, turn_off=True)
                                except Exception as e:
                                    _LOGGER.error(
                                        "PhotoFrameCast: Failed to stop watcher %s from group %s: %s",
                                        watcher, sync_group, e
                                    )

                            # Remove group completely
                            hass.data[DOMAIN]["sync_groups"].pop(sync_group, None)

                        else:
                            # Just remove device from group if not owner
                            group.get("devices", set()).discard(entity_id)
                            group.get("devices_ready", set()).discard(entity_id)

        else:
            # Task is done/cancelled, remove stale entry
            hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)

    # Read all options from service call (watchers will have these ignored)
    folder_path = Path(call.data.get("folder"))
    shuffle = call.data.get("shuffle", True)
    recursive = call.data.get("recursive", True)
    loop_forever = call.data.get("loop", True)
    sort_folder_by_folder = call.data.get("sort_folder_by_folder", True)
    sort_order = call.data.get("sort_order", "alpha")
    resume = call.data.get("resume", True)

    interval_seconds = parse_duration(call.data.get("interval", {"seconds": 5}))
    max_runtime_seconds = parse_duration(call.data.get("max_runtime", {"hours": 12}))
    runtime_str = format_runtime(max_runtime_seconds)

    # Sync group handling
    sync_group = call.data.get("sync_group")
    if not sync_group:  # None or empty string
        _LOGGER.warning(
            "PhotoFrameCast: Sync group selected but no name provided; ignoring."
        )
        sync_group = None

    urls = None  # will be filled based on owner/watcher logic

    if sync_group:
        hass.data.setdefault(DOMAIN, {}).setdefault("sync_groups", {})
        groups = hass.data[DOMAIN]["sync_groups"]
        group = groups.get(sync_group)

        # --- FIXED: first device always becomes owner ---
        is_owner_starting = (group is None) or (group.get("owner") is None) or (group.get("owner") == entity_id)

        if is_owner_starting:
            # OWNER PATH — fully fresh start; collect photos and build URLs
            # Cancel previous tick_task if exists
            if group and group.get("tick_task"):
                try:
                    group["tick_task"].cancel()
                    await group["tick_task"]
                    _LOGGER.info("PhotoFrameCast: Sync group tick updater cancelled for '%s'", sync_group)
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass

            # Validate folder
            if not folder_path.is_dir():
                _LOGGER.warning("PhotoFrameCast: Folder does not exist for owner %s: %s", entity_id, folder_path)
                await notify_user(hass, "PhotoFrameCast Error", f"Folder does not exist: {folder_path}")
                return

            photos = await hass.async_add_executor_job(
                collect_photos, folder_path, recursive, shuffle, sort_folder_by_folder, sort_order
            )
            if not photos:
                _LOGGER.warning("PhotoFrameCast: No photos were found in folder %s", folder_path)
                await notify_user(hass, "PhotoFrameCast Warning", f"No photos found in folder {folder_path}")
                return

            base_url = get_url(hass, prefer_external=True)
            urls = [
                f"{base_url}/api/photoframecast/{entity_id}/{quote(str(p.relative_to(folder_path)).replace('\\', '/'))}"
                for p in photos if p.is_file()
            ]
            if not urls:
                _LOGGER.warning("PhotoFrameCast: No valid photos were found to cast for %s", entity_id)
                await notify_user(hass, "PhotoFrameCast Warning", f"No valid photos found to cast for {entity_id}")
                return

            # Determine resume index for owner
            resume_index = hass.data[DOMAIN]["resume_data"].get(entity_id, 0)
            start_index = 0
            if resume and not shuffle:
                if 0 <= resume_index < len(urls):
                    start_index = resume_index
                    _LOGGER.info("PhotoFrameCast: Resuming %s (owner of sync group '%s') at index %d",
                                 entity_id, sync_group, start_index)
                else:
                    _LOGGER.info(
                        "PhotoFrameCast: Resume index %d invalid for %s (list has %d photos) -> starting from beginning",
                        resume_index, entity_id, len(urls)
                    )
                    start_index = 0

            # Create new group or update existing
            group = {
                "devices": set([entity_id]),
                "devices_ready": set(),
                "tick": start_index,
                "tick_task": None,
                "start_time": datetime.now(),
                "max_runtime": max_runtime_seconds,
                "interval": interval_seconds,
                "owner": entity_id,
                "stopped": False,
                "urls": urls,
                "shuffle": shuffle,
                "loop": loop_forever,
                "recursive": recursive,
                "sort_folder_by_folder": sort_folder_by_folder,
                "sort_order": sort_order,
                "lock": asyncio.Lock(),
            }
            groups[sync_group] = group

            # Launch tick updater with owner's URL count and interval
            group["tick_task"] = hass.async_create_task(
                sync_group_tick_updater(hass, sync_group, len(urls), interval_seconds)
            )
            _LOGGER.info(
                "PhotoFrameCast: Created new sync group '%s' with interval %ss",
                sync_group, interval_seconds
            )

        else:
            # Wait until owner has initialized URLs
            while "urls" not in group:
                await asyncio.sleep(0.1)

            urls = group["urls"]
            interval_seconds = group["interval"]
            loop_forever = group["loop"]
            shuffle = group["shuffle"]
            recursive = group["recursive"]
            sort_folder_by_folder = group["sort_folder_by_folder"]
            sort_order = group["sort_order"]
            max_runtime_seconds = group.get("max_runtime", 0)  # not used by watchers
            shuffle = group["shuffle"]
            resume = False

            # Add device to group as watcher
            group["devices"].add(entity_id)

            # Start index immediately synced to owner's current tick
            start_index = group["tick"]
            _LOGGER.info(
                "PhotoFrameCast: Device %s joined existing sync group '%s' (watcher). Owner options are enforced; any device options are ignored.",
                entity_id, sync_group
            )
            _LOGGER.debug(
                "PhotoFrameCast: watcher %s synced first index=%d with owner %s",
                entity_id, start_index, group["owner"]
            )

    else:
        # Non-sync slideshow: collect photos/URLs normally
        if not folder_path.is_dir():
            _LOGGER.warning("PhotoFrameCast: Folder does not exist: %s", folder_path)
            await notify_user(hass, "PhotoFrameCast Error", f"Folder does not exist: {folder_path}")
            return

        photos = await hass.async_add_executor_job(
            collect_photos, folder_path, recursive, shuffle, sort_folder_by_folder, sort_order
        )
        if not photos:
            _LOGGER.warning("PhotoFrameCast: No photos were found in folder %s", folder_path)
            await notify_user(hass, "PhotoFrameCast Warning", f"No photos found in folder {folder_path}")
            return

        base_url = get_url(hass, prefer_external=True)
        urls = [
            f"{base_url}/api/photoframecast/{entity_id}/{quote(str(p.relative_to(folder_path)).replace('\\', '/'))}"
            for p in photos if p.is_file()
        ]
        if not urls:
            _LOGGER.warning("PhotoFrameCast: No valid photos were found to cast for %s", entity_id)
            await notify_user(hass, "PhotoFrameCast Warning", f"No valid photos found to cast for {entity_id}")
            return

    # At this point, URLs are guaranteed to be ready (owner path or non-sync). For watchers, they were inherited.
    runtime_str = format_runtime(max_runtime_seconds)
    _LOGGER.info(
       "PhotoFrameCast: Slideshow started on %s (%d photos, interval=%ds, max_runtime=%s, "
       "shuffle=%s, sort_folder_by_folder=%s, sort_order=%s, resume=%s, recursive=%s, loop=%s, force=%s, sync_group=%s)",
        entity_id, len(urls), interval_seconds, runtime_str,
        shuffle, sort_folder_by_folder, sort_order, resume, recursive, loop_forever, force, sync_group
    )

    start_index = 0
    resume_index = hass.data[DOMAIN]["resume_data"].get(entity_id, 0)

    if sync_group:
        # This entity is the OWNER of the sync group
        if resume and not shuffle:
            if 0 <= resume_index < len(urls):
                start_index = resume_index
                _LOGGER.info(
                    "PhotoFrameCast: Resuming %s (owner of sync_group '%s') at index %d",
                    entity_id, sync_group, start_index
                )
            else:
                start_index = 0
                _LOGGER.info(
                    "PhotoFrameCast: Resume index %d invalid for %s (owner of sync_group '%s', %d photos) -> starting from beginning",
                    resume_index, entity_id, sync_group, len(urls)
                )
    else:
        # Normal slideshow (no sync group)
        if resume and not shuffle:
            if 0 <= resume_index < len(urls):
                start_index = resume_index
                _LOGGER.info(
                    "PhotoFrameCast: Resuming %s at index %d",
                    entity_id, start_index
                )
            else:
                start_index = 0
                _LOGGER.info(
                    "PhotoFrameCast: Resume index %d invalid for %s (%d photos) -> starting from beginning",
                    resume_index, entity_id, len(urls)
                )

    # --- start slideshow task ---
    task = asyncio.create_task(
        run_slideshow(
            hass,
            entity_id,
            urls,
            interval_seconds,
            loop_forever,
            shuffle,
            force,
            max_runtime_seconds,
            resume,
            sync_group,
            start_index=start_index,
        )
    )

    hass.data[DOMAIN]["running_slideshows"][entity_id] = {
        "task": task,
        "folder": folder_path,
        "urls": urls,
        "max_runtime": max_runtime_seconds,
        "index": 0,
        "sync_group": sync_group,
        "paused": False
    }
    try:
        running_tasks[entity_id] = task
    except Exception:
        _LOGGER.debug("PhotoFrameCast: failed to register running task for %s in running_tasks", entity_id)

# -------------------------
# Other services
# -------------------------
async def stop_slideshow_service(call: ServiceCall):
    """Handle the stop slideshow service call."""
    hass = call.hass
    entity_id = call.data.get("entity_id")
    turn_off = call.data.get("turn_off", True)
    if not entity_id:
        _LOGGER.warning("PhotoFrameCast: No media player was specified; slideshow could not be stopped")
        await notify_user(call.hass, "PhotoFrameCast Warning", "No media player specified; slideshow could not be stopped")
        return
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

    duration_input = call.data.get("max_runtime")
    if not duration_input:
        duration_input = {"hours": 1, "minutes": 0, "seconds": 0}

    max_runtime_seconds = hms_to_seconds(
        hours=duration_input.get("hours", 0),
        minutes=duration_input.get("minutes", 0),
        seconds=duration_input.get("seconds", 0),
    )

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
            _LOGGER.info(
                "PhotoFrameCast: A previous task was running on %s and has been cancelled to start Photo of the Day",
                entity_id,
            )
            try:
                await existing_task
            except asyncio.CancelledError:
                pass
            hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
            if entity_id in running_tasks and running_tasks[entity_id] == existing_task:
                del running_tasks[entity_id]
        else:
            _LOGGER.warning(
                "PhotoFrameCast: Photo of the Day requested for %s but a slideshow is already running, force=False",
                entity_id,
            )
            await notify_user(
                call.hass,
                "PhotoFrameCast Warning",
                f"A slideshow is already running on {entity_id}, but force=False; Photo of the Day will not start",
            )
            return

    _LOGGER.info(
        "PhotoFrameCast: Photo of the Day started on %s (%s, max_runtime=%s, force=%s)",
        entity_id,
        photo.name,
        format_runtime(max_runtime_seconds),
        force,
    )

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
        "task": task,
        "folder": folder_path,
        "urls": urls,
        "max_runtime": max_runtime_seconds,
    }
    try:
        running_tasks[entity_id] = task
    except Exception:
        _LOGGER.debug("PhotoFrameCast: failed to register running task for %s in running_tasks", entity_id)


async def pause_slideshow_service(call: ServiceCall):
    """Pause a currently running slideshow on a media player."""
    hass: HomeAssistant = call.hass
    entity_id = call.data.get("entity_id")
    if not entity_id:
        _LOGGER.error("PhotoFrameCast: No entity_id provided for pause")
        return

    slideshows = hass.data.setdefault(DOMAIN, {}).get("running_slideshows", {})
    task_info = slideshows.get(entity_id)

    if not task_info or not task_info.get("task") or task_info["task"].done():
        _LOGGER.warning("PhotoFrameCast: No running slideshow to pause for %s", entity_id)
        return

    sync_group = task_info.get("sync_group")
    if sync_group:
        group = hass.data[DOMAIN]["sync_groups"].get(sync_group)
        if group:
            async with group["lock"]:
                group["paused"] = True
            _LOGGER.info(
                "PhotoFrameCast: Sync group '%s' paused by device %s", sync_group, entity_id
            )
    else:
        task_info["paused"] = True
        _LOGGER.info("PhotoFrameCast: Slideshow paused on %s", entity_id)


async def resume_slideshow_service(call: ServiceCall):
    """Resume a paused slideshow on a media player."""
    hass: HomeAssistant = call.hass
    entity_id = call.data.get("entity_id")
    if not entity_id:
        _LOGGER.error("PhotoFrameCast: No entity_id provided for resume")
        return

    slideshows = hass.data.setdefault(DOMAIN, {}).get("running_slideshows", {})
    task_info = slideshows.get(entity_id)

    if not task_info:
        _LOGGER.warning("PhotoFrameCast: No paused slideshow found for %s", entity_id)
        return

    sync_group = task_info.get("sync_group")
    if sync_group:
        group = hass.data[DOMAIN]["sync_groups"].get(sync_group)
        if group:
            async with group["lock"]:
                if group.get("paused", False):
                    group["paused"] = False
            _LOGGER.info(
                "PhotoFrameCast: Sync group '%s' resumed by device %s", sync_group, entity_id
            )
    else:
        if task_info.get("paused", False):
            task_info["paused"] = False
            _LOGGER.info("PhotoFrameCast: Slideshow resumed on %s", entity_id)


async def slideshow_loop(hass, entity_id: str, photos: list, interval: int, loop_forever: bool):
    if not photos:
        _LOGGER.warning("PhotoFrameCast: No photos to display for %s", entity_id)
        return

    index = 0
    try:
        while True:
            task_info = hass.data[DOMAIN]["running_slideshows"].get(entity_id)
            if not task_info:
                break

            if task_info.get("paused", False):
                await asyncio.sleep(1)
                continue

            if index >= len(photos):
                if not loop_forever:
                    _LOGGER.info("PhotoFrameCast: Slideshow completed for %s", entity_id)
                    break
                index = 0

            try:
                hass.data[DOMAIN]["current_photo"] = photos[index]
                # TODO: Call your media_player cast logic here
                await asyncio.sleep(interval)
            except Exception as e:
                _LOGGER.error("PhotoFrameCast: Error showing photo #%d for %s: %s", index, entity_id, e)

            index += 1
    except asyncio.CancelledError:
        _LOGGER.info("PhotoFrameCast: Slideshow on %s was cancelled", entity_id)
    finally:
        # Ensure cleanup
        hass.data[DOMAIN]["running_slideshows"].pop(entity_id, None)
        # Remove from running_tasks if still present and owned by current task
        try:
            current = asyncio.current_task()
            if entity_id in running_tasks and running_tasks[entity_id] == current:
                del running_tasks[entity_id]
        except Exception:
            pass
        _LOGGER.info("PhotoFrameCast: Slideshow stopped on %s", entity_id)
