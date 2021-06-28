from typing import Union
from datetime import datetime
from ..env import SERVER


async def dispatch(event: str, time: Union[str, datetime], **kwargs):
    print({"event": event, "time": time, "data": kwargs})
    await SERVER.send_all({"event": event, "time": time, "data": kwargs})
