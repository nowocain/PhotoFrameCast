import logging
import asyncio
import random
import time
from pathlib import Path
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers.network import get_url
from homeassistant.components.http import HomeAssistantView
from aiohttp import web
from .helpers import collect_photos, notify_user
from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# ------------------------------
# Service to start web slideshow
# ------------------------------
async def start_webslideshow_service(call: ServiceCall):
    """Service to start a web slideshow."""
    hass: HomeAssistant = call.hass

    folder_path = Path(call.data.get("folder", "/media/photos"))
    interval = int(call.data.get("interval", 5))
    recursive = call.data.get("recursive", True)
    shuffle = call.data.get("shuffle", True)
    loop_forever = call.data.get("loop", True)
    sort_folder_by_folder = call.data.get("sort_folder_by_folder", True)
    auto_restart = call.data.get("auto_restart", False)

    hass.data.setdefault(DOMAIN, {})
    hass.data[DOMAIN]["auto_restart"] = auto_restart
    hass.data[DOMAIN]["last_slideshow_call"] = time.time()

    if not folder_path.exists() or not folder_path.is_dir():
        _LOGGER.error("PhotoFrameCast: Folder %s does not exist or is not a directory", folder_path)
        return

    # Cancel any existing slideshow loop safely
    existing_task = hass.data[DOMAIN].get("webslideshow_task")
    if existing_task and not existing_task.done():
        _LOGGER.info("PhotoFrameCast: Previous web slideshow is being cancelled.")
        existing_task.cancel()
        try:
            await existing_task
        except asyncio.CancelledError:
            pass

    # Clear leftover state
    hass.data[DOMAIN]["current_photo"] = None
    hass.data[DOMAIN]["web_photos"] = None

    hass.data[DOMAIN]["folder_path"] = folder_path
    hass.data[DOMAIN]["webslideshow_running"] = True
    hass.data[DOMAIN]["webslideshow_interval"] = interval

    # Collect photos
    photos = await asyncio.to_thread(
        collect_photos, folder_path, recursive, False, sort_folder_by_folder
    )
    if not photos:
        _LOGGER.warning("PhotoFrameCast: No photos found in %s", folder_path)
        return

    # Shuffle if requested
    if shuffle:
        random.shuffle(photos)

    hass.data[DOMAIN]["web_photos"] = [str(photo) for photo in photos]

    # Set first photo immediately
    hass.data[DOMAIN]["current_photo"] = photos[0]

    # --------------------------
    # Slideshow loop
    # --------------------------
    async def slideshow_loop():
        try:
            index = 0
            while True:
                if hass.data[DOMAIN].get("webslideshow_task") != asyncio.current_task():
                    break

                if index >= len(photos):
                    if not loop_forever:
                        break
                    index = 0

                hass.data[DOMAIN]["current_photo"] = photos[index]
                await asyncio.sleep(interval)
                index += 1

        except asyncio.CancelledError:
            pass
        finally:
            if hass.data[DOMAIN].get("webslideshow_task") == asyncio.current_task():
                hass.data[DOMAIN]["webslideshow_running"] = False
                hass.data[DOMAIN].pop("webslideshow_task", None)
                if not loop_forever:
                    _LOGGER.info("PhotoFrameCast: Web slideshow finished (all photos displayed).")
                    await notify_user(
                        hass,
                        "PhotoFrameCast Web Slideshow",
                        "Slideshow finished: all photos displayed.",
                        notification_id="photoframecast_webslideshow_finished"
                    )

    task = asyncio.create_task(slideshow_loop())
    hass.data[DOMAIN]["webslideshow_task"] = task

    # --------------------------
    # Optional watchdog for auto-restart
    # --------------------------
    if auto_restart and not hass.data[DOMAIN].get("webslideshow_watchdog_task"):
        async def slideshow_watchdog():
            while True:
                await asyncio.sleep(10)
                data = hass.data.get(DOMAIN, {})
                if data.get("auto_restart") and not data.get("webslideshow_running"):
                    _LOGGER.info("PhotoFrameCast: Slideshow not running, restarting automatically.")
                    folder_path = str(data.get("folder_path", "/media/photos"))
                    interval = data.get("webslideshow_interval", 5)
                    await start_webslideshow_service(
                        ServiceCall(
                            domain=DOMAIN,
                            service="start_webslideshow",
                            data={"folder": folder_path, "interval": interval, "auto_restart": True},
                            context=None
                        )
                    )

        watchdog_task = asyncio.create_task(slideshow_watchdog())
        hass.data[DOMAIN]["webslideshow_watchdog_task"] = watchdog_task

    # Logging and notification
    _LOGGER.info(
        "PhotoFrameCast: Web slideshow started (%d photos, interval=%ds, shuffle=%s, recursive=%s, loop=%s, sort_folder_by_folder=%s, auto_restart=%s)",
        len(photos), interval, shuffle, recursive, loop_forever, sort_folder_by_folder, auto_restart
    )

    base_url = get_url(hass, prefer_external=True)
    url = f"{base_url}/api/photoframecast/webslideshow"
    _LOGGER.info("Access at: %s", url)

    await notify_user(
        hass,
        "PhotoFrameCast Web Slideshow",
        f"Your slideshow is available at: {url}",
        notification_id="photoframecast_webslideshow"
    )

# ------------------------------
# Service to stop web slideshow
# ------------------------------
async def stop_webslideshow_service(call: ServiceCall):
    """Service to stop a web slideshow."""
    hass: HomeAssistant = call.hass
    task = hass.data[DOMAIN].pop("webslideshow_task", None)
    if task:
        task.cancel()
    # Stop watchdog if running
    watchdog_task = hass.data[DOMAIN].pop("webslideshow_watchdog_task", None)
    if watchdog_task:
        watchdog_task.cancel()

    hass.data[DOMAIN]["webslideshow_running"] = False
    hass.data[DOMAIN].pop("current_photo", None)
    hass.data[DOMAIN].pop("web_photos", None)

    _LOGGER.info("PhotoFrameCast: Web slideshow stopped by user.")

# -----------------------
# HTTP Views
# -----------------------
class WebSlideshowView(HomeAssistantView):
    url = "/api/photoframecast/webslideshow"
    name = "api:photoframecast:webslideshow"
    requires_auth = False

    async def get(self, request):
        hass = request.app["hass"]
        interval = hass.data.get(DOMAIN, {}).get("webslideshow_interval", 5)

        html = f"""
        <html>
        <head>
          <title>PhotoFrameCast WebSlideshow</title>
          <style>
            html, body {{
                margin: 0;
                padding: 0;
                background: black;
                width: 100%;
                height: 100%;
                display: flex;
                justify-content: center;
                align-items: center;
            }}
            img {{
                width: 100vw;
                height: 100vh;
                object-fit: contain;
            }}
          </style>
        </head>
        <body>
          <img id="slideshow" src="">
          <script>
            const interval = {interval} * 1000;
            async function updatePhoto() {{
              try {{
                const res = await fetch('/api/photoframecast/webslideshow/current');
                const data = await res.json();
                if (data.photo) {{
                  document.getElementById("slideshow").src = data.photo;
                }}
              }} catch (e) {{
                  console.error("Failed to fetch current photo:", e);
              }}
            }}
            updatePhoto();
            setInterval(updatePhoto, interval);
          </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type="text/html")


class WebSlideshowCurrentView(HomeAssistantView):
    url = "/api/photoframecast/webslideshow/current"
    name = "api:photoframecast:webslideshow:current"
    requires_auth = False

    async def get(self, request):
        hass = request.app["hass"]
        current = hass.data.get(DOMAIN, {}).get("current_photo")
        folder_path = hass.data.get(DOMAIN, {}).get("folder_path")
        if current and folder_path:
            return self.json({"photo": f"/api/photoframecast/webfiles/{current}"})
        return self.json({"photo": None})


class WebFileView(HomeAssistantView):
    url = "/api/photoframecast/webfiles/{file_path:.*}"
    name = "api:photoframecast:webfiles"
    requires_auth = False

    async def get(self, request, file_path):
        hass = request.app["hass"]
        base_folder = hass.data.get(DOMAIN, {}).get("folder_path")
        if not base_folder:
            return web.Response(text="Base folder not set", status=500)

        file = Path(base_folder) / Path(file_path)

        try:
            file.relative_to(base_folder)
        except ValueError:
            return web.Response(text="Forbidden", status=403)

        if not file.exists() or not file.is_file():
            return web.Response(text="File not found", status=404)

        return web.FileResponse(file)
