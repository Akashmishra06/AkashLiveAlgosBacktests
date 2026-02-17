from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError
from configparser import ConfigParser
from functools import lru_cache
import pandas as pd
from typing import Optional

CONFIG_FILE = "/root/Executor_RMS/logics/portfolio_ipad/config.ini"

@lru_cache(maxsize=1)
def _load_config() -> ConfigParser:
    """Load configuration from INI file."""
    cfg = ConfigParser()
    cfg.read(CONFIG_FILE)
    return cfg

@lru_cache(maxsize=4)
def get_mongo_client(section: str) -> MongoClient:
    """
    Create and return a cached MongoDB client for the specified section.
    
    Args:
        section: Configuration section name (e.g., 'MongoAlpha', 'MongoMargin')
    
    Returns:
        MongoClient: Connected MongoDB client
        
    Raises:
        RuntimeError: If connection fails
    """
    cfg = _load_config()
    try:
        client = MongoClient(
            host=cfg.get(section, "host"),
            port=cfg.getint(section, "port"),
            username=cfg.get(section, "username"),
            password=cfg.get(section, "password"),
            authSource=cfg.get(section, "auth_db"),
            serverSelectionTimeoutMS=cfg.getint("MongoCommon", "serverSelectionTimeoutMS"),
            connectTimeoutMS=cfg.getint("MongoCommon", "connectTimeoutMS"),
            socketTimeoutMS=cfg.getint("MongoCommon", "socketTimeoutMS"),
            maxPoolSize=cfg.getint("MongoCommon", "maxPoolSize"),
        )
        # Test connection
        client.admin.command("ping")
        return client
    except ServerSelectionTimeoutError as e:
        raise RuntimeError(f"MongoDB not reachable [{section}]: {e}")
    except Exception as e:
        raise RuntimeError(f"MongoDB connection failed [{section}]: {e}")


def fetch_alpha_cumulative_pnl(clientID: str, limit: int = 300) -> pd.DataFrame:
    """
    Fetch cumulative PnL data for a given client from MongoDB.
    
    Args:
        clientID: Client identifier
        limit: Maximum number of records to fetch (default: 200)
    
    Returns:
        DataFrame with columns: timestamp, date, time, accumulated_pnl, 
                                sum_day, carry_base_for_next_day, datetime
    """
    cfg = _load_config()
    client = get_mongo_client("MongoAlpha")
    db = client[cfg.get("MongoAlpha", "database")]
    col = db[cfg.get("MongoAlpha", "collection")]

    # Fetch data sorted by timestamp descending
    cursor = col.find(
        {"clientID": clientID},
        {
            "_id": 0,
            "timestamp": 1,
            "date": 1,
            "time": 1,
            "accumulated_pnl": 1,
            "sum_day": 1,
            "carry_base_for_next_day": 1,
        }
    ).sort("timestamp", -1).limit(limit)

    data = list(cursor)
    
    if not data:
        return pd.DataFrame()

    # Convert to DataFrame
    df = pd.DataFrame(data)

    # Sort by timestamp ascending (oldest to newest)
    df = df.sort_values("timestamp", ignore_index=True)

    # Create datetime column
    df["datetime"] = pd.to_datetime(
        df["date"].astype(str) + " " + df["time"].astype(str),
        format="%Y%m%d %H:%M:%S",  # Adjust format as needed
        errors="coerce"
    )

    return df


def fetch_alpha_margin(clientID: str) -> Optional[float]:
    """
    Fetch the latest margin for a given client from MongoDB.
    
    Args:
        clientID: Client identifier
    
    Returns:
        Margin value or None if not found
    """
    cfg = _load_config()
    client = get_mongo_client("MongoMargin")
    db = client[cfg.get("MongoMargin", "database")]
    col = db[cfg.get("MongoMargin", "collection")]

    # Find the most recent margin record
    doc = col.find_one(
        {"client": clientID},
        {"_id": 0, "margin": 1}
    )
    
    return doc.get("margin") if doc else None


# # Example usage
# if __name__ == "__main__":
#     clientID = "483874937493"
    
#     # Fetch margin
#     margin = fetch_alpha_margin(clientID)
#     print(f"Client Margin: {margin}")
    
#     # Fetch PnL data
#     df_pnl = fetch_alpha_cumulative_pnl(clientID, limit=200)
#     print(f"\nFetched {len(df_pnl)} PnL records")
#     if not df_pnl.empty:
#         print(df_pnl.head())
#         print(f"\nLatest accumulated PnL: {df_pnl['accumulated_pnl'].iloc[-1]}")