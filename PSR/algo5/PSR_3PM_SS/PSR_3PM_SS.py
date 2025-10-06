from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from configparser import ConfigParser
from datetime import datetime, time
import math


class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        expiryTrue = False
        FirstEntry = False
        avg_premium = None
        breakEven = False

        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 27)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
                self.strategyLogger.info(f"{self.humanTime} | current strangle: {self.openPnl['CurrentPrice'].sum()} | entry strangle: {self.openPnl['EntryPrice'].sum()}")
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                expiryTrue = True

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                expiryTrue = False

            if self.humanTime.time() >= time(15, 26):
                expiryTrue = False
                FirstEntry = False
                breakEven = False

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:

                call_sym_atm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)
                put_sym_atm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)

                for index, row in self.openPnl.iterrows():

                    symbol = row["Symbol"]
                    symSide = symbol[-2:]

                    try:
                        if self.humanTime.time() >= time(15, 25):
                            exitType = f"timeUp,{call_sym_atm},{put_sym_atm}, {row['avg_premium']}"
                            self.exitOrder(index, exitType)

                        if (symSide == "CE" and row['Symbol'] <= call_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                            exitType = f"symSide_call,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                            self.exitOrder(index, exitType)
                            breakEven = True

                        if (symSide == "PE" and row['Symbol'] >= put_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                            exitType = f"symSide_put,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                            self.exitOrder(index, exitType)
                            breakEven = True

                        if breakEven and row['CurrentPrice'] >= row['EntryPrice']:
                            exitType = f"breakEven,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                            self.exitOrder(index, exitType)
                            breakEven = False

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred: {e}")

            if expiryTrue and lastIndexTimeData[1] in df.index and self.openPnl.empty and self.humanTime.time() >= time(15, 0):

                if FirstEntry == False:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium = (data_call_atm["c"] + data_put_atm["c"])

                            symSide_call = call_sym_atm[-7:-2]
                            symSide_put = put_sym_atm[-7:-2]

                            self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_call, "avg_premium": avg_premium})
                            self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_put, "avg_premium": avg_premium})
                            FirstEntry = True
                            breakEven = False

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    global config
    config = ConfigParser()
    config.read('/root/PSR/algo5/config.ini')

    devName = "AM"
    strategyName = config.get('PSR_3PM_SS', 'algoName')
    version = "v1"

    startDate = eval(config.get('PSR_3PM_SS', 'startDate'))
    endDate = eval(config.get('PSR_3PM_SS', 'endDate'))

    algo = algoLogic(devName, strategyName, version)

    baseSym = "SENSEX"
    indexName = "SENSEX"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=True, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")
