import asyncio

DOMAIN = "photoframecast"
STORAGE_KEY = f"{DOMAIN}_resume_data"
STORAGE_VERSION = 1
running_tasks: dict[str, asyncio.Task] = {}
