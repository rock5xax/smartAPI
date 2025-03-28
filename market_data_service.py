import os
import asyncio
import logging
import redis
import requests
import pyotp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv
from typing import Dict, Any, Optional
from contextlib import asynccontextmanager
from fastapi.staticfiles import StaticFiles

# Load environment variables
load_dotenv()

# Logger Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("logs/market_data_service.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("MarketDataService")

# Global auth instance
auth_client = None

# Lifespan event handler
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Handle startup and shutdown events."""
    global auth_client
    try:
        auth_client = AngelOneAuth()
        auth_client.login()
        if not auth_client.jwt_token:
            raise RuntimeError("Authentication failed")
        logger.info("Application startup complete")
        yield
    finally:
        if auth_client:
            auth_client.logout()
            logger.info("Application shutdown: Logged out from Angel One API")

# Initialize FastAPI app with lifespan
app = FastAPI(lifespan=lifespan)
app.mount("/static", StaticFiles(directory="static", check_dir=False), name="static")  # Avoid error if folder missing
try:
    redis_client = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
    redis_client.ping()  # Test connection
except redis.ConnectionError as e:
    logger.warning(f"Redis not available: {e}. Proceeding without Redis.")
    redis_client = None

# Angel One Authentication Class
class AngelOneAuth:
    def __init__(self):
        self.api_key = os.getenv("API_KEY")
        self.user_id = os.getenv("USER_ID")
        self.mpin = os.getenv("MPIN")
        self.otp_token = os.getenv("OTP_TOKEN")
        self.base_url = "https://apiconnect.angelbroking.com"

        if not all([self.api_key, self.user_id, self.mpin, self.otp_token]):
            logger.critical("Missing required credentials in .env file")
            raise ValueError("Missing required credentials in .env file")

        try:
            pyotp.TOTP(self.otp_token).now()
        except Exception:
            logger.critical("Invalid OTP_TOKEN format in .env file")
            raise ValueError("Invalid OTP_TOKEN format in .env file")

        self.refresh_token = None
        self.feed_token = None
        self.jwt_token = None

    def _get_headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "X-UserType": "USER",
            "X-SourceID": "WEB",
            "X-ClientLocalIP": "127.0.0.1",
            "X-ClientPublicIP": "your_public_ip",  # Replace with actual IP if needed
            "X-MACAddress": "00:00:00:00:00:00",
            "X-PrivateKey": self.api_key,
            "Authorization": f"Bearer {self.jwt_token}" if self.jwt_token else ""
        }

    def login(self) -> Optional[Dict[str, Any]]:
        try:
            totp = pyotp.TOTP(self.otp_token).now()
            payload = {
                "clientcode": self.user_id,
                "password": self.mpin,
                "totp": totp
            }
            response = requests.post(
                f"{self.base_url}/rest/auth/angelbroking/user/v1/loginByPassword",
                json=payload,
                headers=self._get_headers()
            )
            session = response.json()
            if response.status_code == 200 and session.get("status"):
                self.jwt_token = session["data"]["jwtToken"]
                self.refresh_token = session["data"]["refreshToken"]
                self.feed_token = session["data"]["feedToken"]
                logger.info("Successfully logged into Angel One API")
                return session
            else:
                logger.error(f"Login failed: {session.get('message', 'Unknown error')}")
                return None
        except Exception as e:
            logger.critical(f"Authentication Error: {e}", exc_info=True)
            return None

    def logout(self) -> Optional[Dict[str, Any]]:
        try:
            payload = {"clientcode": self.user_id}
            response = requests.post(
                f"{self.base_url}/rest/secure/angelbroking/user/v1/logout",
                json=payload,
                headers=self._get_headers()
            )
            result = response.json()
            if response.status_code == 200 and result.get("status"):
                logger.info("Successfully logged out.")
                return result
            else:
                logger.warning(f"Logout failed: {result.get('message', 'No active session')}")
                return None
        except Exception as e:
            logger.error(f"Logout Error: {e}", exc_info=True)
            return None

    def get_ltp_data(self, exchange: str, tradingsymbol: str, symboltoken: str) -> Optional[Dict[str, Any]]:
        try:
            payload = {
                "exchange": exchange,
                "tradingsymbol": tradingsymbol,
                "symboltoken": symboltoken
            }
            response = requests.post(
                f"{self.base_url}/rest/secure/angelbroking/market/v1/quote",
                json={"mode": "LTP", "data": [payload]},
                headers=self._get_headers()
            )
            data = response.json()
            if response.status_code == 200 and data.get("status"):
                return data["data"]["fetched"][0]
            logger.error(f"LTP fetch failed: {data.get('message', 'Unknown error')} - Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"LTP Data Error: {e}", exc_info=True)
            return None

    def get_historical_data(self, exchange: str, symboltoken: str, interval: str, fromdate: str, todate: str) -> Optional[Dict[str, Any]]:
        try:
            payload = {
                "exchange": exchange,
                "symboltoken": symboltoken,
                "interval": interval,
                "fromdate": fromdate,
                "todate": todate
            }
            response = requests.get(
                f"{self.base_url}/rest/secure/angelbroking/historical/v1/getCandleData",
                params=payload,
                headers=self._get_headers()
            )
            data = response.json()
            if response.status_code == 200 and data.get("status"):
                return data["data"]
            logger.error(f"Historical data fetch failed: {data.get('message', 'Unknown error')} - Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Historical Data Error: {e}", exc_info=True)
            return None

    def place_order(self, orderparams: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        try:
            response = requests.post(
                f"{self.base_url}/rest/secure/angelbroking/order/v1/placeOrder",
                json=orderparams,
                headers=self._get_headers()
            )
            data = response.json()
            if response.status_code == 200 and data.get("status"):
                return data["data"]
            logger.error(f"Order placement failed: {data.get('message', 'Unknown error')} - Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Order Placement Error: {e}", exc_info=True)
            return None

    def get_order_book(self) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(
                f"{self.base_url}/rest/secure/angelbroking/order/v1/getOrderBook",
                headers=self._get_headers()
            )
            data = response.json()
            if response.status_code == 200 and data.get("status"):
                return data["data"]
            logger.error(f"Order book fetch failed: {data.get('message', 'Unknown error')} - Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Order Book Error: {e}", exc_info=True)
            return None

    def get_profile(self) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(
                f"{self.base_url}/rest/secure/angelbroking/user/v1/getProfile",
                headers=self._get_headers()
            )
            data = response.json()
            if response.status_code == 200 and data.get("status"):
                return data["data"]
            logger.error(f"Profile fetch failed: {data.get('message', 'Unknown error')} - Response: {response.text}")
            return None
        except Exception as e:
            logger.error(f"Profile Error: {e}", exc_info=True)
            return None

# Root endpoint
@app.get("/")
async def root():
    return {"message": "Welcome to Angel One Market Data Service"}

@app.websocket("/ws/market_data")
async def market_data_ws(websocket: WebSocket):
    await websocket.accept()
    try:
        while True:
            market_data = auth_client.get_ltp_data("NSE", "RELIANCE-EQ", "738561")
            if market_data:
                if redis_client:
                    redis_client.set("market_data", str(market_data))
                await websocket.send_json(market_data)
            else:
                await websocket.send_json({"error": "Failed to fetch market data"})
            await asyncio.sleep(1)
    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket Error: {e}", exc_info=True)
        await websocket.close()

@app.get("/market_data")
async def get_market_data():
    if not redis_client:
        return {"data": "Redis not available, start WebSocket to fetch live data"}
    data = redis_client.get("market_data")
    return {"data": data if data else "No data available"}

@app.get("/historical_data")
async def get_historical():
    data = auth_client.get_historical_data(
        exchange="NSE",
        symboltoken="738561",
        interval="ONE_MINUTE",
        fromdate="2025-03-27 09:00",
        todate="2025-03-27 15:30"  # Adjusted to market close
    )
    return {"historical_data": data if data else "No data available"}

@app.get("/order_book")
async def get_orders():
    data = auth_client.get_order_book()
    return {"order_book": data if data else "No data available"}

@app.get("/profile")
async def get_user_profile():
    data = auth_client.get_profile()
    return {"profile": data if data else "No data available"}

@app.post("/place_order")
async def place_new_order():
    order_params = {
        "variety": "NORMAL",
        "tradingsymbol": "RELIANCE-EQ",
        "symboltoken": "738561",
        "transactiontype": "BUY",
        "exchange": "NSE",
        "ordertype": "MARKET",
        "producttype": "INTRADAY",
        "duration": "DAY",
        "quantity": "1"
    }
    data = auth_client.place_order(order_params)
    return {"order": data if data else "Order placement failed"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)