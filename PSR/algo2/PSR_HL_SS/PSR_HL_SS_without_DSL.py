from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time
import math

class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = (int(getExpiryData(self.timeData, baseSym)["LotSize"]))
        expiryTrue = False
        entryTrigger = False
        exitTrigger = False
        avg_premium = None
        FirstEntry = False
        NewStraddle = None
        checkStraddle = 0
        EnterOneTime = False

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)

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
                expiryTrue = True

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()
                expiryTrue = False

            if lastIndexTimeData[1] in df.index and ((self.humanTime.time() <= time(15, 15) and expiryTrue) or entryTrigger):
                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        NewStraddle = data_call_atm["c"] + data_put_atm["c"]

                        # Track the day's high straddle
                        if self.humanTime.time() == time(9, 16): # NewStraddle > checkStraddle:
                            checkStraddle = NewStraddle

                        # Trigger EnterOneTime only once per day
                        if not EnterOneTime and NewStraddle <= (checkStraddle - 100):
                            EnterOneTime = True
                            self.strategyLogger.info(f"{self.humanTime}, EnterOneTime TRIGGERED, Straddle dropped to half of the high: {NewStraddle:.2f} <= {checkStraddle-100:.2f}")

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")

            if lastIndexTimeData[1] in df.index and ((self.humanTime.time() <= time(15, 15) and expiryTrue) or entryTrigger):

                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]
                    nowcheck = (math.ceil(avg_premium / 100) * 100) - 100

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        avg_premium1 = ((data_call_atm["c"] + data_put_atm["c"]))
                        if avg_premium1 < nowcheck:

                            call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium1, Currentexpiry, 0)
                            put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium1, Currentexpiry, 0)
                            data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                            data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])
                            if data_call_atm_2 is not None and data_put_atm_2 is not None:
                                avg_premium = avg_premium1
                                exitTrigger = True
                                self.strategyLogger.info(f"{self.humanTime}, exitTrigger")

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")

            if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                call_sym_atm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry, 0)
                put_sym_atm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry, 0)

                if self.humanTime.time() >= time(15, 15):
                    for index, row in self.openPnl.iterrows():
                        exitType = f"timeUp,{call_sym_atm},{put_sym_atm}, {row['avg_premium']}"
                        self.exitOrder(index, exitType)
                    expiryTrue = False

                else:
                    for index, row in self.openPnl.iterrows():
                        symbol = row["Symbol"]
                        symSide = symbol[-2:]

                        try:
                            entry_sum = self.openPnl['EntryPrice'].sum()
                            current_sum = self.openPnl['CurrentPrice'].sum()

                            if exitTrigger and current_sum < (0.25 * entry_sum) and (len(self.openPnl) == 2):
                                for index, row in self.openPnl.iterrows():
                                    exitType = f"exit,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                                    self.exitOrder(index, exitType, row['CurrentPrice'])

                            self.strategyLogger.info(f"{self.humanTime}, {entry_sum}, {current_sum}, {(0.25 * entry_sum)}")
                            if exitTrigger and (len(self.openPnl) == 1):
                                for index, row in self.openPnl.iterrows():
                                    exitType = f"exit_single_trade,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                                    self.exitOrder(index, exitType, row['CurrentPrice'])

                            if symSide == "CE" and row['Symbol'] <= call_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']:
                                exitType = f"symSide_call,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                                self.exitOrder(index, exitType)

                            if symSide == "PE" and row['Symbol'] >= put_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']:
                                exitType = f"symSide_put,{call_sym_atm},{put_sym_atm},{row['avg_premium']}"
                                self.exitOrder(index, exitType)

                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

            if self.humanTime.time() > time(15, 20):
                avg_premium = None
                avg_premium1 = None
                FirstEntry = False
                EnterOneTime = False
                NewStraddle = None
                checkStraddle = 0

            if EnterOneTime == True:
                if FirstEntry == True and lastIndexTimeData[1] in df.index and (self.openPnl.empty) and (self.humanTime.time() != time(9, 16)) and ((self.humanTime.time() <= time(15, 15) and expiryTrue) and exitTrigger or entryTrigger):

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        nowcheck = (math.ceil(avg_premium / 100) * 100) - 100

                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium = (data_call_atm["c"] + data_put_atm["c"])

                            call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium, Currentexpiry, 0)
                            put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium, Currentexpiry, 0)

                            symSide_call = call_sym_atm_2[-7:-2]
                            symSide_put = put_sym_atm_2[-7:-2]

                            data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                            data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])
                            if data_call_atm_2 is not None and data_put_atm_2 is not None:

                                self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_call, "avg_premium": avg_premium})
                                self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_put, "avg_premium": avg_premium})
                                exitTrigger = False

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

                if FirstEntry == False and lastIndexTimeData[1] in df.index and expiryTrue or entryTrigger:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium = (data_call_atm["c"] + data_put_atm["c"])

                            call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium, Currentexpiry, 0)
                            put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium, Currentexpiry, 0)

                            symSide_call = call_sym_atm_2[-7:-2]
                            symSide_put = put_sym_atm_2[-7:-2]

                            data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                            data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])
                            if data_call_atm_2 is not None and data_put_atm_2 is not None:

                                self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_call, "avg_premium": avg_premium})
                                self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {"Expiry": expiryEpoch, "symSide": symSide_put, "avg_premium": avg_premium})
                                exitTrigger = False
                                FirstEntry = True

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "PSR_HL_SS_without_DSL"
    version = "v1"

    startDate = datetime(2024, 1, 1, 9, 15)
    endDate = datetime(2025, 8, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "SENSEX"
    indexName = "SENSEX"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")