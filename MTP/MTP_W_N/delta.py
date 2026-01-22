from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time
import pandas as pd
import numpy as np
import talib


class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "1Min")
            df_15Min = getFnoBacktestData(indexSym, startEpoch-(86400*100), endEpoch, "15Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df_15Min["datetime"] = pd.to_datetime(df_15Min["datetime"])
        df_15Min = df_15Min.sort_values("datetime")

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

        df_15Min['callSell'] = np.where((df_15Min['percent_buy'] < df_15Min['percent_sell']), "callSell", "")
        df_15Min['putSell'] = np.where((df_15Min['percent_buy'] > df_15Min['percent_sell']), "putSell", "")

        df_15Min = df_15Min[df_15Min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")

        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        lastEntry = False
        pnll = None

        nine15Price = None
        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            timeEpochSubstract = (timeData-900)
            if timeEpochSubstract in df_15Min.index:
                last15MinIndexTimeData.pop(0)
                last15MinIndexTimeData.append(timeEpochSubstract)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]

                        if row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.4):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.5
                            self.strategyLogger.info(f"SL2 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.5}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.6
                            self.strategyLogger.info(f"SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.6}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.7):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                            self.strategyLogger.info(f"SL4 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")

                    except Exception as e:
                        self.strategyLogger.info(e)

            self.pnlCalculator()

            if self.humanTime.time() == time(9, 17):
                Currentexpiry = getExpiryData(self.timeData+(86400*1), baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    self.openPnl['EntryTime'] = pd.to_datetime(self.openPnl['EntryTime'])
                    lastDate = self.openPnl['EntryTime'].dt.date.max()

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

                    elif row["CurrentPrice"] <= row["Target"]:
                        exitType = f"TargetHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = f"StoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

            self.openPnl['opt_type'] = self.openPnl['Symbol'].str[-2:]

            tradecount=self.openPnl['opt_type'].value_counts()
            callCounter=tradecount.get('CE', 0)
            putCounter=tradecount.get('PE', 0)

            if self.humanTime.time() == time(9, 16):
                nine15Price = df.at[lastIndexTimeData[1], "c"]
            # if self.openPnl.empty:
            #     lastEntry = True
            #     pnll = None

            # if not self.openPnl.empty:
            #     self.openPnl['EntryTime'] = pd.to_datetime(self.openPnl['EntryTime'], errors='coerce')
            #     pnll = self.openPnl['Pnl'].sum()
            #     lastDate = self.openPnl['EntryTime'].dt.date.max()
            #     if self.humanTime.date() > lastDate:
            #         lastEntry = True
            #     else:
            #         lastEntry = False

            if (timeEpochSubstract in df_15Min.index) and (self.humanTime.time() < time(15, 15)):

                if callCounter == 0 and nine15Price is not None and nine15Price > df_15Min.at[last15MinIndexTimeData[1], "c"] and df_15Min.at[last15MinIndexTimeData[1], "callSell"] == "callSell":

                    try:
                        callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])

                        otm = 0
                        while data["c"] > 400:
                            otm += 1
                            putSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                            otm += 1

                        otm = 0
                        while data["c"] < 100:
                            otm -= 1
                            callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            otm -= 1

                        target = 0.2 * data["c"]
                        stoploss = 1.3 * data["c"]
                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "stoploss2": df_15Min.at[last15MinIndexTimeData[1], "h"]})
                    except Exception as e:
                        self.strategyLogger.info(e)

                if putCounter == 0 and nine15Price is not None and nine15Price < df_15Min.at[last15MinIndexTimeData[1], "c"] and df_15Min.at[last15MinIndexTimeData[1], "putSell"] == "putSell":

                    try:
                        callSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])

                        otm = 0
                        while data["c"] > 400:
                            otm += 1
                            putSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                            otm += 1

                        otm = 0
                        while data["c"] < 100:
                            otm -= 1
                            callSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            otm -= 1

                        target = 0.2 * data["c"]
                        stoploss = 1.3 * data["c"]

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "stoploss2": df_15Min.at[last15MinIndexTimeData[1], "l"]})
                    except Exception as e:
                        self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "delta"
    version = "v1"

    startDate = datetime(2025, 12, 1, 9, 15)
    endDate = datetime(2026, 1, 15, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")