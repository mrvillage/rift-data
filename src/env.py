__version__ = "Alpha 3.1.0"

from asyncio import get_event_loop
from datetime import datetime
import os
from typing import TYPE_CHECKING
from dotenv import load_dotenv
from .server import SocketServer

load_dotenv()

TOKEN = os.getenv("Rift_discord_token")
PATH = f"{os.getcwd()}"
FOOTER = os.getenv("footer")
COLOR = os.getenv("color")
APIKEY = os.getenv("pnw_api_key")
EMAIL = os.getenv("pnw_email")
PASSWORD = os.getenv("pnw_password")
BASEURL = "https://politicsandwar.com/api"
GQL_URL = f"https://api.politicsandwar.com/graphql?api_key={APIKEY}"
DB_HOST = os.getenv("db_host")
DB_PORT = os.getenv("db_port")
DB_USER = os.getenv("db_user")
DB_PASSWORD = os.getenv("db_password")
DB_NAME = os.getenv("db_name")
AUTH_CODE = os.getenv("auth_code")
SOCKET_PORT = os.getenv("socket_port")

if TYPE_CHECKING:
    assert isinstance(AUTH_CODE, str)
    assert SOCKET_PORT is not None
    assert COLOR is not None

COLOR = int(COLOR)
SOCKET_PORT = int(SOCKET_PORT)

SERVER = SocketServer(auth_code=AUTH_CODE, loop=get_event_loop(), port=SOCKET_PORT)

# add database and event update stuff to the end of the setters
class UpdateTime:
    def __init__(self):
        self.alliances = None
        self.cities = None
        self.colors = None
        self.completed_trades = None
        self.market_prices = None
        self.nations = None
        self.pending_trades = None
        self.prices = None
        self.treasures = None
        self.treaties = None
        self.wars = None

    async def set_alliances(self, value: datetime):
        self.alliances = value

    async def set_cities(self, value: datetime):
        self.cities = value

    async def set_colors(self, value: datetime):
        self.colors = value

    async def set_completed_trades(self, value: datetime):
        self.completed_trades = value

    async def set_market_prices(self, value: datetime):
        self.market_prices = value

    async def set_nations(self, value: datetime):
        self.nations = value

    async def set_pending_trades(self, value: datetime):
        self.pending_trades = value

    async def set_prices(self, value: datetime):
        self.prices = value

    async def set_treasures(self, value: datetime):
        self.treasures = value

    async def set_treaties(self, value: datetime):
        self.treaties = value

    async def set_wars(self, value: datetime):
        self.wars = value


UPDATE_TIMES = UpdateTime()
