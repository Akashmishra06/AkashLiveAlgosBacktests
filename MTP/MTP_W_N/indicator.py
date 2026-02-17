import pandas as pd
import numpy as np
import talib


def calculate_market_2H_strength(df_15Min: pd.DataFrame) -> pd.DataFrame:
    """
    Takes 15Min timeframe dataframe and:
    1. Resamples into custom market-based 2H candles:
        09:15-11:15
        11:15-13:15
        13:15-15:15
        15:15-15:30 (treated as separate block)
    2. Calculates MA + MACD strength
    3. Shifts(1) to avoid forward bias
    4. Maps percent_buy & percent_sell back to 15Min dataframe
    5. Returns final df_15Min
    """

    df = df_15Min.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime")
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time

    # -----------------------------------
    # 1️⃣ Create custom 2H session blocks
    # -----------------------------------

    def assign_block(row):
        t = row["datetime"].time()

        if t >= pd.to_datetime("09:15").time() and t < pd.to_datetime("11:15").time():
            return 1
        elif t >= pd.to_datetime("11:15").time() and t < pd.to_datetime("13:15").time():
            return 2
        elif t >= pd.to_datetime("13:15").time() and t < pd.to_datetime("15:15").time():
            return 3
        else:
            # 15:15 – 15:30 (last candle)
            return 4

    df["block"] = df.apply(assign_block, axis=1)

    # Unique session id per day per block
    df["session_id"] = df["date"].astype(str) + "_" + df["block"].astype(str)

    # -----------------------------------
    # 2️⃣ Aggregate into 2H candles
    # -----------------------------------

    df_2H = (
        df.groupby("session_id")
        .agg({
            "datetime": "last",
            "o": "first",
            "h": "max",
            "l": "min",
            "c": "last",
            "v": "sum"
        })
        .sort_values("datetime")
    )

    df_2H = df_2H.set_index("datetime")

    # -----------------------------------
    # 3️⃣ Indicator Calculation
    # -----------------------------------

    def get_ma(series, length):
        return talib.SMA(series, timeperiod=length)

    def macd_pair(series, fast, slow):
        macd, signal, hist = talib.MACD(
            series,
            fastperiod=fast,
            slowperiod=slow,
            signalperiod=9
        )
        return hist

    df_2H["ma20"]  = get_ma(df_2H["c"], 20)
    df_2H["ma50"]  = get_ma(df_2H["c"], 50)
    df_2H["ma100"] = get_ma(df_2H["c"], 100)
    df_2H["ma150"] = get_ma(df_2H["c"], 150)
    df_2H["ma200"] = get_ma(df_2H["c"], 200)

    df_2H["hist20_50"]   = macd_pair(df_2H["c"], 20, 50)
    df_2H["hist20_100"]  = macd_pair(df_2H["c"], 20, 100)
    df_2H["hist20_200"]  = macd_pair(df_2H["c"], 20, 200)
    df_2H["hist50_100"]  = macd_pair(df_2H["c"], 50, 100)
    df_2H["hist50_200"]  = macd_pair(df_2H["c"], 50, 200)
    df_2H["hist100_200"] = macd_pair(df_2H["c"], 100, 200)
    df_2H["hist50_150"]  = macd_pair(df_2H["c"], 50, 150)

    ma_cols = ["ma20", "ma50", "ma100", "ma150", "ma200"]
    macd_cols = [
        "hist20_50", "hist20_100", "hist20_200",
        "hist50_100", "hist50_200",
        "hist100_200", "hist50_150"
    ]

    # MA Buy Count
    df_2H["ma_buy"] = (
        df_2H["c"].values.reshape(-1, 1) >
        df_2H[ma_cols].values
    ).sum(axis=1)

    # MACD Buy Count
    df_2H["macd_buy"] = (
        df_2H[macd_cols].values > 0
    ).sum(axis=1)

    df_2H["percent_buy"] = (
        (df_2H["ma_buy"] + df_2H["macd_buy"]) / 12
    ) * 100

    df_2H["percent_sell"] = 100 - df_2H["percent_buy"]

    # 🔴 Avoid forward bias
    df_2H[["percent_buy", "percent_sell"]] = (
        df_2H[["percent_buy", "percent_sell"]].shift(1)
    )

    # -----------------------------------
    # 4️⃣ Map back to 15Min dataframe
    # -----------------------------------

    df_2H_map = df_2H[["percent_buy", "percent_sell"]]

    df = df.merge(
        df_2H_map,
        left_on="datetime",
        right_index=True,
        how="left"
    )

    # Forward fill within each session
    df["percent_buy"] = df.groupby("session_id")["percent_buy"].ffill()
    df["percent_sell"] = df.groupby("session_id")["percent_sell"].ffill()

    # Clean helper columns
    df = df.drop(columns=["date", "time", "block", "session_id"])

    return df
