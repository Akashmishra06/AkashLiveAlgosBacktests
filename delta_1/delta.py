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

        df_15Min["rsi"] = talib.RSI(df_15Min["c"], timeperiod=14)

        df_15Min['callBuy'] = np.where((df_15Min['rsi'] > 30) & (df_15Min['rsi'].shift(1) < 30) & (df_15Min['c'] > df_15Min['o']), "callBuy", "")
        df_15Min['putBuy'] = np.where((df_15Min['rsi'] < 70) & (df_15Min['rsi'].shift(1) > 70) & (df_15Min['c'] < df_15Min['o']), "putBuy", "")

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

                        # if row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                        #     self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        # elif row['CurrentPrice'] < (row['EntryPrice'] * 0.4):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.5
                        #     self.strategyLogger.info(f"SL2 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.5}")

                        # elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.6
                        #     self.strategyLogger.info(f"SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.6}")

                        # elif row['CurrentPrice'] < (row['EntryPrice'] * 0.7):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                        #     self.strategyLogger.info(f"SL4 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")

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

                    # if self.timeData >= row["Expiry"]:
                    if self.humanTime.time() >= time(15, 15):
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

                    elif row["CurrentPrice"] >= row["Target"]:
                        exitType = f"TargetHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] <= row["Stoploss"]:
                        exitType = f"StoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif (timeEpochSubstract in df_15Min.index):

                        if (df_15Min.at[last15MinIndexTimeData[1], "o"] > df_15Min.at[last15MinIndexTimeData[1], "c"]) & (symSide == "CE"):
                            exitType = "CE_exit"
                            self.exitOrder(index, exitType)

                        elif (df_15Min.at[last15MinIndexTimeData[1], "o"] < df_15Min.at[last15MinIndexTimeData[1], "c"]) & (symSide == "PE"):
                            exitType = "PE_exit"
                            self.exitOrder(index, exitType)

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter= tradecount.get('CE',0)
            putCounter= tradecount.get('PE',0)

            if (timeEpochSubstract in df_15Min.index) and self.humanTime.time() <= time(14, 0):

                if df_15Min.at[last15MinIndexTimeData[1], "callBuy"] == "callBuy" and callCounter == 0:

                    try:
                        putSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])

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

                        target = 5 * data["c"]
                        stoploss = 0.8 * data["c"]
                        self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                    except Exception as e:
                        self.strategyLogger.info(e)

                elif df_15Min.at[last15MinIndexTimeData[1], "putBuy"] == "putBuy" and putCounter == 0:

                    try:
                        callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])

                        otm = 0
                        while data["c"] > 400:
                            otm += 1
                            callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            otm += 1

                        otm = 0
                        while data["c"] < 100:
                            otm -= 1
                            callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            otm -= 1

                        target = 5 * data["c"]
                        stoploss = 0.8 * data["c"]
                        self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                    except Exception as e:
                        self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "delta1"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 15, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")