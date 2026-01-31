import talib

def addIndicatorsAndSignals(df_15Min):
    df = df_15Min.copy()

    df["rsi"] = talib.RSI(df["c"], timeperiod=7)

    rsi_min = df["rsi"].rolling(window=7, min_periods=7).min()
    rsi_max = df["rsi"].rolling(window=7, min_periods=7).max()

    df["stoch_rsi"] = (df["rsi"] - rsi_min) / (rsi_max - rsi_min)
    df["stoch_rsi_k"] = df["stoch_rsi"] * 100
    df["stoch_rsi_k_smooth"] = df["stoch_rsi_k"].rolling(window=3, min_periods=3).mean()
    df["stoch_rsi_d"] = df["stoch_rsi_k_smooth"].rolling(window=3, min_periods=3).mean()

    k = df["stoch_rsi_k_smooth"]


    df["callsell"] = 0
    df["putsell"] = 0

    call_state = 0
    put_state = 0

    for i in range(1, len(df)):
        prev_k = k.iloc[i - 1]
        curr_k = k.iloc[i]

        if call_state == 0 and curr_k < 20:
            call_state = 1
        elif call_state == 1 and curr_k > 80:
            call_state = 2
        elif call_state == 2 and prev_k >= 80 and curr_k < 80:
            df.at[df.index[i], "callsell"] = 1
            call_state = 0

        if put_state == 0 and curr_k > 80:
            put_state = 1
        elif put_state == 1 and curr_k < 20:
            put_state = 2
        elif put_state == 2 and prev_k <= 20 and curr_k > 20:
            df.at[df.index[i], "putsell"] = 1
            put_state = 0

    return df