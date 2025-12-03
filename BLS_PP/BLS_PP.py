from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time


class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch-(86400*5), endEpoch, "1Min")
            df_5Min = getFnoBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "5Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df_5Min = df_5Min[df_5Min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_5Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_5Min.csv")

        lastIndexTimeData = [0, 0]
        last5MinIndexTimeData = [0, 0]

        MonthlyExpiry = getExpiryData(startEpoch, baseSym)['MonthLast']
        expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
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
            timeEpochSubstract = (timeData-300)
            if timeEpochSubstract in df_5Min.index:
                last5MinIndexTimeData.pop(0)
                last5MinIndexTimeData.append(timeEpochSubstract)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                        if (timeEpochSubstract in df_5Min.index):
                            self.strategyLogger.info(f"datetime: {self.humanTime}, {row['Symbol']}, {row['EntryPrice']}, {row['CurrentPrice']}, {row['Quantity']}")
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                MonthlyExpiry = getExpiryData(self.timeData+86400, baseSym)['MonthLast']
                expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                self.strategyLogger.info(f"datetime: {self.humanTime}, MonthlyExpiry Update: {MonthlyExpiry}")

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

            if (timeEpochSubstract in df_5Min.index) and self.openPnl.empty:

                try:
                    putSym = self.getPutSym(self.timeData, baseSym, df_5Min.at[last5MinIndexTimeData[1], "c"], MonthlyExpiry, 0, 100)
                    data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                    self.entryOrder(data["c"], putSym, lotSize, "BUY", {"Expiry": expiryEpoch})
                except Exception as e:
                    self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "BLS_PP"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 11, 30, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=True, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")