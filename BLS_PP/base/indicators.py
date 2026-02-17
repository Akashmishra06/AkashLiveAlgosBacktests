from numba import njit
import pandas as pd
import numpy as np
import talib


@njit(cache=True)
def generate_signals(k):
    n = len(k)
    callsell = np.zeros(n, dtype=np.int8)
    putsell = np.zeros(n, dtype=np.int8)
    call_state = 0
    put_state = 0

    for i in range(1, n):
        prev_k = k[i - 1]
        curr_k = k[i]
        if np.isnan(curr_k):
            continue

        if call_state == 0 and curr_k < 20:
            call_state = 1
        elif call_state == 1 and curr_k > 80:
            call_state = 2
        elif call_state == 2 and prev_k >= 80 and curr_k < 80:
            callsell[i] = 1
            call_state = 0

        if put_state == 0 and curr_k > 80:
            put_state = 1
        elif put_state == 1 and curr_k < 20:
            put_state = 2
        elif put_state == 2 and prev_k <= 20 and curr_k > 20:
            putsell[i] = 1
            put_state = 0

    return callsell, putsell


def addIndicatorsAndSignals(df):
    df = df.copy()
    close = np.asarray(df["c"].values, dtype=np.float64)

    rsi = talib.RSI(close, timeperiod=7).astype(np.float32)
    df["rsi"] = rsi

    rsi_series = pd.Series(rsi)
    rsi_min = rsi_series.rolling(window=7, min_periods=7).min().values
    rsi_max = rsi_series.rolling(window=7, min_periods=7).max().values

    stoch_rsi = (rsi - rsi_min) / (rsi_max - rsi_min)
    stoch_rsi_k = stoch_rsi * 100

    k_smooth = pd.Series(stoch_rsi_k).rolling(window=3, min_periods=3).mean().values
    d_line = pd.Series(k_smooth).rolling(window=3, min_periods=3).mean().values

    df["stoch_rsi"] = stoch_rsi
    df["stoch_rsi_k"] = stoch_rsi_k
    df["stoch_rsi_k_smooth"] = k_smooth
    df["stoch_rsi_d"] = d_line

    callsell, putsell = generate_signals(k_smooth)
    df["callSell"] = callsell
    df["putSell"] = putsell

    return df


def getFinalStrike(timeDate, lastIndexTimeData, baseSym, indexPrice, Expiry,
                   AimedOTM, strikeDiff, premLimit1, premiumLimit2, side,
                   getCallSym, getPutSym, fetchAndCacheFnoHistData, strategyLogger=None):
    """
    Optimized function to fetch option symbol with premium within given limits.
    """
    try:
        if side == "CE":
            getSym = getCallSym
            otm = 0
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
        elif side == "PE":
            getSym = getPutSym
            otm = AimedOTM
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
        else:
            return None

        data_obj = fetchAndCacheFnoHistData(sym, lastIndexTimeData)

        while data_obj["c"] > premiumLimit2 or data_obj["c"] < premLimit1:
            if data_obj["c"] > premiumLimit2:
                otm += 1
            elif data_obj["c"] < premLimit1:
                otm -= 1
            sym = getSym(timeDate, baseSym, indexPrice, Expiry, otm, strikeDiff)
            data_obj = fetchAndCacheFnoHistData(sym, lastIndexTimeData)

        return sym

    except Exception as e:
        if strategyLogger:
            strategyLogger.info(e)
        return None



def AdvancedMAAndMACD(df_2H):

    def get_ma(series, length):
        return talib.SMA(series, timeperiod=length)

    def macd_pair(series, fast, slow):
        macd, signal, hist = talib.MACD(series, fastperiod=fast, slowperiod=slow, signalperiod=9)
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

    ma_cols = ["ma20","ma50","ma100","ma150","ma200"]
    macd_cols = ["hist20_50","hist20_100","hist20_200","hist50_100","hist50_200","hist100_200","hist50_150"]

    df_2H["ma_buy"] = (df_2H["c"].to_numpy().reshape(-1,1) > df_2H[ma_cols].to_numpy()).sum(axis=1)
    df_2H["macd_buy"] = (df_2H[macd_cols].to_numpy() > 0).sum(axis=1)

    df_2H["percent_buy"] = ((df_2H["ma_buy"] + df_2H["macd_buy"]) / 12) * 100
    df_2H["percent_sell"] = 100 - df_2H["percent_buy"]

    df_2H['percent_buy'] = df_2H['percent_buy'].shift(1)
    df_2H['percent_sell'] = df_2H['percent_sell'].shift(1)
    return df_2H