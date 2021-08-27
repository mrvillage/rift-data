import asyncio
import ssl
from typing import TYPE_CHECKING, Any
from aiohttp import web
from aiohttp.http_websocket import WSMsgType
from aiohttp.web_request import BaseRequest


class SocketServer:
    def __init__(
        self, *, auth_code: str, loop: asyncio.AbstractEventLoop, port: int = 9093
    ):
        self.auth_code = auth_code
        self.endpoints = {}
        self.sockets = []
        self.server = None
        self.loop = loop
        self.port = port
        self.runner = None
        self.site = None

    def route(self, name=None):
        def decorator(func):
            if not name:
                self.endpoints[func.__name__] = func
            else:
                self.endpoints[name] = func
            return func

        return decorator

    async def socket_handler(self, req: BaseRequest) -> None:
        websocket = web.WebSocketResponse()
        await websocket.prepare(req)
        self.sockets.append(
            {
                "socket": websocket,
            }
        )
        async for message in websocket:
            try:
                request: dict = message.json()
                endpoint = request.get("endpoint")
                code = request.get("code")
                if not code or code != self.auth_code:
                    response = {
                        "success": False,
                        "error": "Invalid token.",
                    }
                elif not endpoint or endpoint not in self.endpoints:
                    response = {"success": False, "error": "Invalid endpoint."}
                else:
                    args = request.get("args", [])
                    kwargs = request.get("kwargs", {})
                    try:
                        response = {
                            "success": True,
                            "data": await self.endpoints[endpoint](*args, *kwargs),
                        }
                    except Exception as error:
                        response = {"success": False, "error": str(error)}
                try:
                    await websocket.send_json(response)
                except Exception as error:
                    response = {"success": False, "error": str(error)}
                    await websocket.send_json(response)
            except Exception as error:
                for sock in self.sockets:
                    if sock["socket"] is websocket:
                        self.sockets.remove(sock)
                return

    async def start_coro(self) -> None:
        if TYPE_CHECKING:
            assert self.server is not None
        self.runner = web.AppRunner(self.server)
        await self.runner.setup()
        self.site = web.TCPSite(
            self.runner,
            "*",
            self.port,
            # ssl_context=ssl.create_default_context(ssl.Purpose.CLIENT_AUTH),
        )
        await self.site.start()

    def start(self) -> None:
        self.server = web.Application()
        self.server.router.add_route("GET", "/", self.socket_handler)  # type: ignore
        self.loop.create_task(self.start_coro())

    async def send_all(self, data: dict[str, Any]) -> None:
        coros = [socket["socket"].send_json(data) for socket in self.sockets]
        await asyncio.gather(*coros)
