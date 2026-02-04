
    def compute_indicator(self, df, timeframe="60Min", ma_type="SMA"):
        df_tf = self.resample_ohlc(df.copy(), timeframe)
        # ---- 1) Close Price ----
        close_tf = df_tf["c"]

        # ---- 2) MAs ----
        ma20  = self.get_ma(close_tf, 20, ma_type)
        ma50  = self.get_ma(close_tf, 50, ma_type)
        ma100 = self.get_ma(close_tf, 100, ma_type)
        ma150 = self.get_ma(close_tf, 150, ma_type)
        ma200 = self.get_ma(close_tf, 200, ma_type)

        # ---- 3) MACDs ----
        hist20_50   = self.macd(close_tf, 20, 50)
        hist20_100  = self.macd(close_tf, 20, 100)
        hist20_200  = self.macd(close_tf, 20, 200)
        hist50_100  = self.macd(close_tf, 50, 100)
        hist50_200  = self.macd(close_tf, 50, 200)
        hist100_200 = self.macd(close_tf, 100, 200)
        hist50_150  = self.macd(close_tf, 50, 150)

        # ---- 4) Combine into one DF for alignment ----
        result = pd.DataFrame({
            "ti": df_tf.index,
            "c": close_tf,
            "ma20": ma20,
            "ma50": ma50,
            "ma100": ma100,
            "ma150": ma150,
            "ma200": ma200,
            "hist20_50": hist20_50,
            "hist20_100": hist20_100,
            "hist20_200": hist20_200,
            "hist50_100": hist50_100,
            "hist50_200": hist50_200,
            "hist100_200": hist100_200,
            "hist50_150": hist50_150,
        })

        # ---- 5) Bullish Conditions ----
        cond = (
            (result["c"] > result["ma20"]).astype(int) +
            (result["c"] > result["ma50"]).astype(int) +
            (result["c"] > result["ma100"]).astype(int) +
            (result["c"] > result["ma150"]).astype(int) +
            (result["c"] > result["ma200"]).astype(int) +
            (result["hist20_50"] > 0).astype(int) +
            (result["hist20_100"] > 0).astype(int) +
            (result["hist20_200"] > 0).astype(int) +
            (result["hist50_100"] > 0).astype(int) +
            (result["hist50_200"] > 0).astype(int) +
            (result["hist100_200"] > 0).astype(int) +
            (result["hist50_150"] > 0).astype(int)
        )

        result["percent_buy"] = (cond / 12) * 100
        result["percent_sell"] = 100 - result["percent_buy"]
        result.set_index('ti', inplace=True)
        # ---- 6) Reindex back to original DF ----
        df["percent_buy"]  = result["percent_buy"].reindex(df.index).ffill()
        df["percent_sell"] = result["percent_sell"].reindex(df.index).ffill()
        df = self.compute_trend(df)
        return df





and this is another way

        def get_ma(series, length):
            return talib.SMA(series, timeperiod=length)

        def macd_pair(series, fast, slow):
            macd, signal, hist = talib.MACD(series, fastperiod=fast, slowperiod=slow, signalperiod=9)
            return hist

        df_2H = df_15Min.resample("120T", on="datetime").agg({"o":"first","h":"max","l":"min","c":"last","v":"sum"}).dropna()

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

        df_merge = df_2H[["percent_buy","percent_sell"]]

        df_15Min["merge_key"] = df_15Min["datetime"].dt.floor("120T")
        df_15Min = df_15Min.merge(df_merge, left_on="merge_key", right_index=True, how="left")
        df_15Min.drop(columns=["merge_key"], inplace=True)

which one is correct 