from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time, timedelta
import numpy as np
import talib

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df['rsi'] = talib.RSI(df['c'], timeperiod=14)

        df['callSell'] = np.where((df['rsi'] < 50) & (df['c'] < df['c'].shift(1)) & ((df['rsi'] > 30)), "callSell", "")
        df['putSell'] = np.where((df['rsi'] > 50) & (df['c'] > df['c'].shift(1)) & ((df['rsi'] < 70)), "putSell", "")

        df['callExit'] = np.where((df['rsi'] > 70), "callExit", "")
        df['putExit'] = np.where((df['rsi'] < 30), "putExit", "")

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        avg_premium = None

        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
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

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()

            if self.humanTime.date() >= expiryDatetime.date() - timedelta(days=15):
                entry = 50
            else:
                entry = 100

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                for index, row in self.openPnl.iterrows():

                    symbol = row["Symbol"]
                    symSide = symbol[-2:]

                    try:
                        if self.humanTime.time() >= time(15, 15):
                            exitType = f"timeUp, {row['avg_premium']}, {row['avg_premium_1']}"
                            self.exitOrder(index, exitType)

                        elif symSide == "CE" and df.at[lastIndexTimeData[1], "callExit"] == "callExit" and row['EntryPrice'] < row['CurrentPrice']:
                            exitType = f"callRsiExit, {row['avg_premium']}, {row['avg_premium_1']}"
                            self.exitOrder(index, exitType)

                        elif symSide == "PE" and df.at[lastIndexTimeData[1], "putExit"] == "putExit" and row['EntryPrice'] < row['CurrentPrice']:
                            exitType = f"putRsiExit, {row['avg_premium']}, {row['avg_premium_1']}"
                            self.exitOrder(index, exitType)

                        elif symSide == "CE" and (row['EntryPrice'] * 0.1) > row['CurrentPrice']:
                            exitType = f"Target, {row['avg_premium']}, {row['avg_premium_1']}"
                            self.exitOrder(index, exitType)

                        elif symSide == "PE" and (row['EntryPrice'] * 0.1) > row['CurrentPrice']:
                            exitType = f"Target, {row['avg_premium']}, {row['avg_premium_1']}"
                            self.exitOrder(index, exitType)

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred: {e}")

            putTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('PE', 0)
            callTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('CE', 0)

            if lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and self.humanTime.time() > time(9, 17):

                # ---------------- PUT SELL ----------------
                if df.at[lastIndexTimeData[1], "putSell"] == "putSell" and putTradeCounter == 0:
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium_1 = data_call_atm["c"] + data_put_atm["c"]
                            avg_premium = avg_premium_1 * 0.1  # 10% of straddle premium
                            taking_price = avg_premium
                            otm = 0

                            while otm <= 50:
                                try:
                                    putSym = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, otm, entry)
                                    data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                                except Exception as e:
                                    self.strategyLogger.info(e)
                                    self.strategyLogger.info(f"Data not found for {putSym}")
                                    otm += 1
                                    continue

                                if data is not None and data["c"] < taking_price:
                                    self.entryOrder(
                                        data["c"], putSym, lotSize, "SELL",
                                        {"Expiry": expiryEpoch, "avg_premium": avg_premium, "avg_premium_1": avg_premium_1}
                                    )
                                    self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {putSym}, entryPrice: {data['c']}")
                                    break

                                otm += 1

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                # ---------------- CALL SELL ----------------
                if df.at[lastIndexTimeData[1], "callSell"] == "callSell" and callTradeCounter == 0:
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium_1 = data_call_atm["c"] + data_put_atm["c"]
                            avg_premium = avg_premium_1 * 0.1  # 10% of straddle premium
                            taking_price = avg_premium
                            otm = 0

                            while otm <= 50:
                                try:
                                    callSym = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, otm, entry)
                                    data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                                except Exception as e:
                                    self.strategyLogger.info(e)
                                    self.strategyLogger.info(f"Data not found for {callSym}")
                                    otm += 1
                                    continue

                                if data is not None and data["c"] < taking_price:
                                    self.entryOrder(
                                        data["c"], callSym, lotSize, "SELL",
                                        {"Expiry": expiryEpoch, "avg_premium": avg_premium, "avg_premium_1": avg_premium_1}
                                    )
                                    self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {callSym}, entryPrice: {data['c']}")
                                    break

                                otm += 1

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "RMS_SS"
    version = "v1"

    startDate = datetime(2025, 9, 1, 9, 15)
    endDate = datetime(2025, 9, 29, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "SENSEX"
    indexName = "SENSEX"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")