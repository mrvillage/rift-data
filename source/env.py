__version__ = "Alpha 3.1.0"

from asyncio import get_event_loop
from datetime import datetime
import os
from dotenv import load_dotenv
from .server import SocketServer

load_dotenv()

TOKEN = os.getenv("Rift_discord_token")
PATH = f"{os.getcwd()}"
FOOTER = os.getenv("footer")
COLOR = int(os.getenv("color"))
APIKEY = os.getenv("pnw_api_key")
EMAIL = os.getenv("pnw_email")
PASSWORD = os.getenv("pnw_password")
BASEURL = "https://politicsandwar.com/api"
GQLURL = f"https://api.politicsandwar.com/graphql?api_key={APIKEY}"
DBHOST = os.getenv("db_host")
DBPORT = os.getenv("db_port")
DBUSER = os.getenv("db_user")
DBPASSWORD = os.getenv("db_password")
DBNAME = os.getenv("db_name")
AUTH_CODE = os.getenv("auth_code")
SOCKET_PORT = int(os.getenv("socket_port"))

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
        self.cities = value

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
