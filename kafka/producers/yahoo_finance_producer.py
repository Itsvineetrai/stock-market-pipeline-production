import yfinance as yf
import logging
import time

from datetime import datetime
from producer_config import get_producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

producer = get_producer()

TICKERS = [
    "AAPL",
    "MSFT",
    "GOOGL",
    "AMZN",
    "TSLA"
]

while True:

    try:

        for ticker in TICKERS:

            stock = yf.Ticker(ticker)

            info = stock.fast_info

            message = {
                "ticker": ticker,
                "price": info.get("lastPrice"),
                "timestamp": datetime.utcnow().isoformat()
            }

            producer.send(
                "stock-prices",
                value=message
            )

            logging.info(message)

        producer.flush()

        time.sleep(5)

    except Exception as e:
        logging.error(e)
        time.sleep(10)