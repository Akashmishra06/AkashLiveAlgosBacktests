from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm

import talib as ta
import numpy as np
from datetime import datetime, time



class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1H")
            if df is None:
                return
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df['ema_10'] = ta.EMA(df['c'], timeperiod=10)
        df['putEntry'] = np.where((df['ema_10'] < df['ema_10'].shift(1)), "putEntry", "")
        df['callEntry'] = np.where((df['ema_10'] > df['ema_10'].shift(1)), "callEntry", "")

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Min.csv")

        lastIndexTimeData = [0, 0]

        MonthlyExpiry = getExpiryData(startEpoch, baseSym)['MonthLast']
        expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        entryTrigger = False

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 3600)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue
            
            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                MonthlyExpiry = getExpiryData(self.timeData+86400, baseSym)['MonthLast']
                expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty and (timeData-3600 in df.index):
                for index, row in self.openPnl.iterrows():
                    symbol = row['Symbol']
                    symSide = str(symbol[-2:])
                    # print(symSide)

                    # if self.humanTime.time() > time(15, 15):
                    #     exitType = f"IntradayTimeUp"
                    #     self.exitOrder(index, exitType, row['CurrentPrice'])

                    if symSide == "CE":
                        if df.at[lastIndexTimeData[1], "putEntry"] == "putEntry":
                            exitType = f"rsiTarget"
                            self.exitOrder(index, exitType, row['CurrentPrice'])

                    elif symSide == "PE":
                        if df.at[lastIndexTimeData[1], "callEntry"] == "callEntry":
                            exitType = f"rsiTarget"
                            self.exitOrder(index, exitType, row['CurrentPrice'])

            if (timeData-3600 in df.index) and (self.openPnl.empty) and (self.humanTime.time() < time(15, 0)):

                if df.at[lastIndexTimeData[1], "putEntry"] == "putEntry":

                    putSym = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], MonthlyExpiry, 0, 100)
                    data = None

                    try:
                        data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                        self.strategyLogger.info(f"Data not found for {putSym}")
                        continue

                    if data is not None:
                        self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch})

                elif df.at[lastIndexTimeData[1], "callEntry"] == "callEntry":

                    callSym = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], MonthlyExpiry, 0, 100)
                    data = None

                    try:
                        data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                    except Exception as e:
                        self.strategyLogger.info(e)
                        self.strategyLogger.info(f"Data not found for {callSym}")
                        continue

                    if data is not None:
                        self.entryOrder(data["c"], callSym, lotSize, "BUY", {"Expiry": expiryEpoch})

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "NA"
    strategyName = "BLS_reserch_cc"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 9, 30, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")

    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")