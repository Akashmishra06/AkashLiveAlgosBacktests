import talib
from numba import njit
import pandas as pd
import numpy as np


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

    rsi = talib.RSI(close, timeperiod=7)
    df["rsi"] = rsi.astype(np.float32)

    stoch_rsi_k, stoch_rsi_d = talib.STOCHRSI(
        close,
        timeperiod=7,     # RSI period
        fastk_period=3,   # %K smoothing
        fastd_period=3,   # %D smoothing
        fastd_matype=0    # SMA
    )

    df["stoch_rsi_k"] = stoch_rsi_k.astype(np.float32)
    df["stoch_rsi_d"] = stoch_rsi_d.astype(np.float32)

    k_smooth = stoch_rsi_k

    callsell, putsell = generate_signals(k_smooth)
    df["callSell"] = callsell
    df["putSell"] = putsell

    return df

def getFinalStrike(timeDate, lastIndexTimeData, last15MinIndexTimeData, baseSym, indexPrice, Expiry,
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