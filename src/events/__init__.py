from datetime import datetime
from typing import Union

from ..env import SERVER


async def dispatch(event: str, time: Union[str, datetime], **kwargs):
    await SERVER.send_all({"event": event, "time": time, "data": kwargs})
