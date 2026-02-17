import pandas as pd
import numpy as np
import talib


def calculate_market_2H_strength(df_15Min: pd.DataFrame) -> pd.DataFrame:
    """
    Exact Python equivalent of the Pine Script indicator.

    Pine Script logic (what we must replicate):
    ─────────────────────────────────────────────
      time_frame = "60"  (standard 60-min / 2H candles)

      ma20  = request.security(..., time_frame, ta.sma(close, 20))
      hist  = request.security(..., time_frame, macd_hist)
      ...
      above_count += (close > ma20  ? 1 : 0)   ← 15Min close vs 2H MA value
      above_count += (hist20_50 > 0 ? 1 : 0)   ← 2H histogram sign
      percent_buy = above_count / 12 * 100

    Key points:
      1. MAs and MACD histograms are computed on the 2H candle series.
      2. request.security() with lookahead=off → during the FORMING 2H candle,
         all 15Min bars see the value from the LAST CLOSED 2H candle.
         → replicated by shift(1) on the 2H series, then map to 15Min.
      3. "close > ma20" uses the CURRENT 15Min bar's close, NOT the 2H close.
         → so we map the raw MA / histogram VALUES down to 15Min, then
           compute the score row-by-row using df["c"] (15Min close).

    Resampling (2H blocks anchored at 09:15):
        09:15 – 11:14  →  block starting 09:15
        11:15 – 13:14  →  block starting 11:15
        13:15 – 15:14  →  block starting 13:15
        15:15 onwards  →  block starting 15:15  (partial/last candle of day)

    Returns:
        Original df_15Min with original index preserved + two new columns:
        percent_buy, percent_sell
    """

    # ------------------------------------------------------------------ #
    #  0. Internal working copy — clean integer index for safe ops        #
    # ------------------------------------------------------------------ #
    df = df_15Min.copy()
    df["datetime"] = pd.to_datetime(df["datetime"])
    df = df.sort_values("datetime").reset_index(drop=True)

    # ------------------------------------------------------------------ #
    #  1. Label each 15Min candle with the 2H block it belongs to        #
    #     Anchor = 09:15, block width = 120 minutes                      #
    # ------------------------------------------------------------------ #
    anchor_min = 9 * 60 + 15   # 09:15 in minutes from midnight

    def _2h_block_start(dt):
        m = dt.hour * 60 + dt.minute
        slot = (m - anchor_min) // 120
        bm   = anchor_min + slot * 120
        return dt.replace(hour=bm // 60, minute=bm % 60, second=0, microsecond=0)

    df["_2h_start"] = df["datetime"].apply(_2h_block_start)
    df["_2h_id"]    = df["_2h_start"].astype(str)

    # ------------------------------------------------------------------ #
    #  2. Aggregate 15Min → 2H candles                                   #
    # ------------------------------------------------------------------ #
    df_2H = (
        df.groupby("_2h_id", sort=True)
        .agg(
            datetime =("_2h_start", "first"),
            o        =("o",         "first"),
            h        =("h",         "max"),
            l        =("l",         "min"),
            c        =("c",         "last"),
            v        =("v",         "sum"),
        )
        .sort_values("datetime")
        .reset_index()
    )

    # ------------------------------------------------------------------ #
    #  3. Compute MA and MACD histogram values on the 2H series          #
    #     talib.SMA  → matches Pine Script ta.sma()                      #
    #     talib.MACD → uses EMA internally, matches Pine Script macd()   #
    # ------------------------------------------------------------------ #
    close_2h = df_2H["c"]

    df_2H["ma20"]  = talib.SMA(close_2h, timeperiod=20)
    df_2H["ma50"]  = talib.SMA(close_2h, timeperiod=50)
    df_2H["ma100"] = talib.SMA(close_2h, timeperiod=100)
    df_2H["ma150"] = talib.SMA(close_2h, timeperiod=150)
    df_2H["ma200"] = talib.SMA(close_2h, timeperiod=200)

    def _hist(s, fast, slow, sig=9):
        _, _, h = talib.MACD(s, fastperiod=fast, slowperiod=slow, signalperiod=sig)
        return h

    df_2H["hist20_50"]   = _hist(close_2h, 20,  50)
    df_2H["hist20_100"]  = _hist(close_2h, 20,  100)
    df_2H["hist20_200"]  = _hist(close_2h, 20,  200)
    df_2H["hist50_100"]  = _hist(close_2h, 50,  100)
    df_2H["hist50_200"]  = _hist(close_2h, 50,  200)
    df_2H["hist100_200"] = _hist(close_2h, 100, 200)
    df_2H["hist50_150"]  = _hist(close_2h, 50,  150)

    # ------------------------------------------------------------------ #
    #  4. Shift(1) on 2H → mirrors request.security() lookahead=off      #
    #     While 2H candle N is forming, 15Min bars see values from N-1.  #
    # ------------------------------------------------------------------ #
    value_cols = [
        "ma20", "ma50", "ma100", "ma150", "ma200",
        "hist20_50", "hist20_100", "hist20_200",
        "hist50_100", "hist50_200", "hist100_200", "hist50_150",
    ]
    df_2H[value_cols] = df_2H[value_cols].shift(1)


    # ------------------------------------------------------------------ #
    #  5. Map the shifted 2H values down to every 15Min candle           #
    #     All 15Min candles in the same 2H block get the SAME values.    #
    # ------------------------------------------------------------------ #
    id_map = df_2H.set_index("_2h_id")[value_cols]

    for col in value_cols:
        df[col] = df["_2h_id"].map(id_map[col])

    # ------------------------------------------------------------------ #
    #  6. Score each 15Min bar using ITS OWN CLOSE (Pine Script exact)   #
    #                                                                      #
    #  Pine Script:  above_count += (close > ma20 ? 1 : 0)               #
    #  → 15Min close  vs  2H MA value (which is now in df["ma20"] etc.)  #
    # ------------------------------------------------------------------ #
    ma_cols   = ["ma20", "ma50", "ma100", "ma150", "ma200"]
    macd_cols = [
        "hist20_50", "hist20_100", "hist20_200",
        "hist50_100", "hist50_200", "hist100_200", "hist50_150",
    ]

    # 15Min close > 2H MA  →  1 point each (max 5)
    ma_buy = (
        df["c"].values.reshape(-1, 1) > df[ma_cols].values
    ).sum(axis=1)

    # 2H MACD histogram > 0  →  1 point each (max 7)
    macd_buy = (df[macd_cols].values > 0).sum(axis=1)

    df["percent_buy"]  = (ma_buy + macd_buy) / 12 * 100
    df["percent_sell"] = 100 - df["percent_buy"]
    # df.to_csv("debug_indicator.csv", index=False)  # debug output

    # ------------------------------------------------------------------ #
    #  7. Write only the two result columns back to original df          #
    #     .values → positional, original index is fully preserved        #
    # ------------------------------------------------------------------ #
    result = df_15Min.copy()
    result["percent_buy"]  = df["percent_buy"].values
    result["percent_sell"] = df["percent_sell"].values

    return result