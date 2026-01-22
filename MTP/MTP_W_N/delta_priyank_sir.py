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

        df_15Min["RSI"] = talib.RSI(df_15Min["c"], timeperiod=7)

        rsi_min = df_15Min["RSI"].rolling(7).min()
        rsi_max = df_15Min["RSI"].rolling(7).max()

        df_15Min["StochRSI"] = (df_15Min["RSI"] - rsi_min) / (rsi_max - rsi_min)
        df_15Min["StochRSI_K"] = df_15Min["StochRSI"] * 100
        df_15Min["StochRSI_K_Smoothed"] = df_15Min["StochRSI_K"].rolling(3).mean()
        df_15Min["StochRSI_D"] = df_15Min["StochRSI_K_Smoothed"].rolling(3).mean()

        # df_15Min.dropna(inplace=True)
        df_15Min['callSell'] = np.where((df_15Min['percent_buy'] < df_15Min['percent_sell']), "callSell", "")
        df_15Min['putSell'] = np.where((df_15Min['percent_buy'] > df_15Min['percent_sell']), "putSell", "")

        df_15Min['putStochCrossOver'] = np.where((df_15Min['StochRSI_K_Smoothed'] > 20) & (df_15Min['StochRSI_K_Smoothed'].shift(1) <= 20), "putStochCrossOver", "")
        df_15Min['callStochCrossOver'] = np.where((df_15Min['StochRSI_K_Smoothed'] < 80) & (df_15Min['StochRSI_K_Smoothed'].shift(1) >= 80), "callStochCrossOver", "")

        df_15Min = df_15Min[df_15Min.index > startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")

        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]

        callEntry = False
        stageOne_c = False
        putEntry = False
        stageOne_p = False

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

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

            if self.humanTime.time() == time(15, 15):
                Currentexpiry = getExpiryData(self.timeData+(86400*4), baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

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

                    elif (timeEpochSubstract in df_15Min.index):

                        if (df_15Min.at[last15MinIndexTimeData[1], "putSell"] == "putSell") & (symSide == "CE"):
                            exitType = "CE_exit"
                            self.exitOrder(index, exitType)

                        elif (df_15Min.at[last15MinIndexTimeData[1], "callSell"] == "callSell") & (symSide == "PE"):
                            exitType = "PE_exit"
                            self.exitOrder(index, exitType)

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            if (timeEpochSubstract in df_15Min.index):

                if df_15Min.at[last15MinIndexTimeData[1], "StochRSI_K_Smoothed"] < 20:
                    stageOne_c = True
                if stageOne_c and df_15Min.at[last15MinIndexTimeData[1], "StochRSI_K_Smoothed"] > 50:
                    stageOne_c = False
                    callEntry = True

                if df_15Min.at[last15MinIndexTimeData[1], "StochRSI_K_Smoothed"] > 80:
                    stageOne_p = True
                if stageOne_p and df_15Min.at[last15MinIndexTimeData[1], "StochRSI_K_Smoothed"] < 50:
                    stageOne_p = False
                    putEntry = True

                if df_15Min.at[last15MinIndexTimeData[1], "putSell"] == "putSell" and df_15Min.at[last15MinIndexTimeData[1], "putStochCrossOver"] == "putStochCrossOver" and callCounter == 0 and putEntry and putCounter <= 2:

                    try:
                        putSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                        target = 0.2 * data["c"]
                        stoploss = 1.5 * data["c"]

                        otm = 0
                        while data["c"] > 400:
                            otm += 1
                            putSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                            target = 0.3 * data["c"]
                            stoploss = 1.5 * data["c"]
                            otm += 1

                        otm = 0
                        while data["c"] < 100:
                            otm -= 1
                            callSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            target = 0.3 * data["c"]
                            stoploss = 1.5 * data["c"]
                            otm -= 1

                        self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                        putEntry = False
                    except Exception as e:
                        self.strategyLogger.info(e)

                elif df_15Min.at[last15MinIndexTimeData[1], "callSell"] == "callSell" and df_15Min.at[last15MinIndexTimeData[1], "callStochCrossOver"] == "callStochCrossOver" and putCounter == 0 and callEntry and callCounter <= 2:

                    try:
                        callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                        target = 0.2 * data["c"]
                        stoploss = 1.5 * data["c"]

                        otm = 0
                        while data["c"] > 400:
                            otm += 1
                            putSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                            target = 0.3 * data["c"]
                            stoploss = 1.5 * data["c"]
                            otm += 1

                        otm = 0
                        while data["c"] < 100:
                            otm -= 1
                            callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            target = 0.3 * data["c"]
                            stoploss = 1.5 * data["c"]
                            otm -= 1

                        self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                        callEntry = False
                    except Exception as e:
                        self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "delta_2"
    version = "v1"

    startDate = datetime(2022, 1, 1, 9, 15)
    endDate = datetime(2025, 12, 15, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")