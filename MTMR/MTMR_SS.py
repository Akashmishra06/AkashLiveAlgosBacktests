from backtestTools.histData import getFnoBacktestData, connectToMongo
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time
import numpy as np
import talib
import pandas as pd


class algoLogic(optOverNightAlgoLogic):

    global conn
    conn = connectToMongo()

    def fetchAndCacheFnoHistData(self, symbol, timestamp, maxCacheSize=100, conn=conn):

        if len(self.symbolDataCache) > maxCacheSize:
            symbolToDelete = []

            for sym in self.symbolDataCache.keys():
                idx = next(i for i, char in enumerate(sym) if char.isdigit())
                optionExpiry = (datetime.strptime(sym[idx:idx + 7], "%d%b%y").timestamp() + 55800)

                if self.timeData > optionExpiry:
                    symbolToDelete.append(sym)

            if symbolToDelete:
                for sym in symbolToDelete:
                    del self.symbolDataCache[sym]

        if symbol in self.symbolDataCache.keys():
            return self.symbolDataCache[symbol].loc[timestamp]

        else:
            idx = next(i for i, char in enumerate(symbol) if char.isdigit())
            optionExpiry = (datetime.strptime(symbol[idx:idx + 7], "%d%b%y").timestamp() + 55800)
            self.symbolDataCache[symbol] = getFnoBacktestData(symbol, timestamp, optionExpiry, "1Min", conn)

            return self.symbolDataCache[symbol].loc[timestamp]


    def run(self, startDate, endDate, baseSym, indexSym):

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch-(86400*20), endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df["datetime"] = pd.to_datetime(df["datetime"])
        df["date"] = df["datetime"].dt.date
        df["time"] = df["datetime"].dt.time

        day_open_map = (df[df["time"] == time(9, 15)].set_index("date")["o"])
        df["dayopen"] = df["date"].map(day_open_map)

        df.dropna(inplace=True)
        df = df[df.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        multiple = 100
        candleColor = None
        onlyOne = False

        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() >= time(15, 27)):
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
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                multiple = 500

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                for index, row in self.openPnl.iterrows():

                    symbol = row["Symbol"]
                    symSide = symbol[-2:]

                    try:
                        if self.timeData >= row["Expiry"]:
                            exitType = f"timeUp,{row['dayOpen']}"
                            self.exitOrder(index, exitType)

                        elif (row['EntryPrice'] * 1.5) <= row['CurrentPrice']:
                            exitType = f"Stoploss,{row['dayOpen']}"
                            self.exitOrder(index, exitType)

                        elif (row['EntryPrice'] * 0.2) >= row['CurrentPrice']:
                            exitType = f"Target,{row['dayOpen']}"
                            self.exitOrder(index, exitType)

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} exit block Exception occurred: {e}")

            putTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('PE', 0)
            callTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('CE', 0)


            if self.humanTime.time() > time(15, 25):
                onlyOne = False
                if df.at[lastIndexTimeData[1], "dayopen"] >= df.at[lastIndexTimeData[1], "c"]:
                    candleColor = "red"
                else:
                    candleColor = "green"

            if lastIndexTimeData[1] in df.index and self.humanTime.time() > time(15, 25):

                if putTradeCounter < 4 and candleColor == "green":

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0, multiple)
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_put_atm is not None:
                            self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {put_sym_atm}, entryPrice: {data_put_atm['c']}")

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                elif callTradeCounter < 4 and candleColor == "red":
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0, multiple)
                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None:

                            self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {call_sym_atm}, entryPrice: {data_call_atm['c']}")

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")

            if onlyOne == False and candleColor is not None and lastIndexTimeData[1] in df.index and self.humanTime.time() >= time(9, 16) and self.humanTime.time() <= time(15, 16):

                if putTradeCounter < 4 and candleColor == "red":

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0, multiple)
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_put_atm is not None:
                            self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {put_sym_atm}, entryPrice: {data_put_atm['c']}")
                            onlyOne = True

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                elif callTradeCounter < 4 and candleColor == "green":
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0, multiple)
                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        if data_call_atm is not None:

                            self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {call_sym_atm}, entryPrice: {data_call_atm['c']}")
                            onlyOne = True

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")

            if self.humanTime.time() > time(15, 25):
                multiple = 100

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "MTMR_SS"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 29, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "SENSEX"
    indexName = "SENSEX"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="1Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")