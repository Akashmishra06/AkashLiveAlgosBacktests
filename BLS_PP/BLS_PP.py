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
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
            if df is None:
                return            
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_{startDate.date()}_1Min.csv")

        lastIndexTimeData = [0, 0]

        MonthlyExpiry = getExpiryData(startEpoch, baseSym)['MonthLast']
        expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            # print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    if self.timeData > row["Expiry"]:
                        try:
                            data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                            self.openPnl.at[index, "CurrentPrice"] = data["c"]
                        except Exception as e:
                            # self.strategyLogger.info(e)
                            print(e)

            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                MonthlyExpiry = getExpiryData(self.timeData+86400, baseSym)['MonthLast']
                expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    if self.timeData > row["Expiry"]:
                        exitType = f"ExpiryExit"
                        self.exitOrder(index, exitType)

            if (timeData-60 in df.index) and (self.openPnl.empty) and (self.humanTime.time() >= time(15, 21)):

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

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "NA"
    strategyName = "BLS_PP"
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