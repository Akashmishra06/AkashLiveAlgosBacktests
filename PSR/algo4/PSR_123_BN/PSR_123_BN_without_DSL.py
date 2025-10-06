from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData, connectToMongo
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time
import math
import pandas as pd


class algoLogic(optOverNightAlgoLogic):
    def run(self, startDate, endDate, baseSym, indexSym):   

        conn = connectToMongo()

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min", conn=conn)
        except Exception as e:
            self.strategyLogger.info(f"Data not found for spot {baseSym}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]
        Currentexpiry = getExpiryData(startEpoch, baseSym)['MonthLast']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        expiryTrue = False
        FirstEntry = False
        exitTrigger = False
        avg_premium = None
        algoOneOpenDf = None
        algoTwoOpenDf = None
        algoOneEntryTrue = True

        NewStraddle = None
        checkStraddle = 0
        EnterOneTime = False
        nineSixteenHighStraddle = None
        avg_premium_algo_2 = None

        secondEntryFirstOnly = False
        exitTrigger_algo_2 = False

        secondEntryFirstOnly_third = False
        EnterOneTime_third = False
        NewStraddle_third = None
        checkStraddle_third = 0

        algoTwoEntryTrue = False
        algoThreeEntryTrue = False

        DayOverLimit = False
        closedPnlList = []
        # datSLValue = 6000
        FinalSl = 6000
        exitTrigger_algo_3 = False


        for timeData in df.index:
            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) or (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if (self.humanTime.time() < time(9, 16)) or (self.humanTime.time() > time(15, 25)):
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
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['MonthLast']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()
                expiryTrue = False

            if self.humanTime.time() > time(15, 20):
                expiryTrue = False
                FirstEntry = False
                exitTrigger = False
                algoOneEntryTrue = True
                EnterOneTime = False
                NewStraddle = None
                checkStraddle = 0

                EnterOneTime_third = False
                NewStraddle_third = None
                checkStraddle_third = 0
                
                avg_premium_algo_2 = None
                exitTrigger_algo_2 = False
                closedPnlList = []
                DayOverLimit = False

                exitTrigger_algo_3 = False
                algoThreeEntryTrue = False

            if lastIndexTimeData[1] in df.index and ((self.humanTime.time() <= time(15, 15) and expiryTrue)):
                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        NewStraddle_third = data_call_atm["c"] + data_put_atm["c"]
                        self.strategyLogger.info(f"{self.humanTime}, straddle: {NewStraddle_third}")

                        if NewStraddle_third > checkStraddle_third:
                            checkStraddle_third = NewStraddle_third

                        if self.humanTime.time() == time(9, 16):
                            nineSixteenHighStraddle = NewStraddle_third
                            self.strategyLogger.info(f"{self.humanTime}, nineSixteenHighStraddle: {nineSixteenHighStraddle:.2f}")

                        if nineSixteenHighStraddle is not None and not EnterOneTime_third and NewStraddle_third <= (checkStraddle_third * 0.7) and nineSixteenHighStraddle == checkStraddle_third:
                            EnterOneTime_third = True
                            secondEntryFirstOnly_third = True
                            self.strategyLogger.info(f"{self.humanTime}, nineSixteenHighStraddle and EnterOneTime TRIGGERED, Straddle dropped to half of the high: {NewStraddle_third:.2f} <= {checkStraddle_third*0.7:.2f}")

                        if not EnterOneTime_third and NewStraddle_third <= (checkStraddle_third * 0.5):
                            EnterOneTime_third = True
                            secondEntryFirstOnly_third = True
                            self.strategyLogger.info(f"{self.humanTime}, EnterOneTime TRIGGERED, Straddle dropped to half of the high: {NewStraddle_third:.2f} <= {checkStraddle_third*0.7:.2f}")

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")


            if secondEntryFirstOnly_third and lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and DayOverLimit == False and expiryTrue:
                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]
                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        avg_premium_algo_3 = (data_call_atm["c"] + data_put_atm["c"])

                        call_sym_atm_3 = self.getCallSym(self.timeData, baseSym, (underlying_price + avg_premium_algo_3), Currentexpiry)
                        put_sym_atm_3 = self.getPutSym(self.timeData, baseSym, (underlying_price - avg_premium_algo_3), Currentexpiry)

                        symSide_call = call_sym_atm_3[-7:-2]
                        symSide_put = put_sym_atm_3[-7:-2]

                        data_call_atm_3 = self.fetchAndCacheFnoHistData(call_sym_atm_3, lastIndexTimeData[1])
                        data_put_atm_3 = self.fetchAndCacheFnoHistData(put_sym_atm_3, lastIndexTimeData[1])
                        if data_call_atm_3 is not None and data_put_atm_3 is not None:
                            self.entryOrder(data_call_atm_3["c"], call_sym_atm_3, lotSize, "SELL", {
                                "Expiry": expiryEpoch,
                                "symSide": symSide_call,
                                "avg_premium": avg_premium_algo_3,
                                "algoName": "three"})
                            
                            self.entryOrder(data_put_atm_3["c"], put_sym_atm_3, lotSize, "SELL", {
                                "Expiry": expiryEpoch,
                                "symSide": symSide_put,
                                "avg_premium": avg_premium_algo_3,
                                "algoName": "three"})

                            # exitTrigger = False
                            secondEntryFirstOnly_third = False

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")


            if lastIndexTimeData[1] in df.index and ((self.humanTime.time() <= time(15, 15) and expiryTrue)):
                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        NewStraddle = data_call_atm["c"] + data_put_atm["c"]

                        if self.humanTime.time() == time(9, 16): # NewStraddle > checkStraddle:
                            checkStraddle = NewStraddle

                        # Trigger EnterOneTime only once per day
                        if not EnterOneTime and NewStraddle <= (checkStraddle - 100):
                            EnterOneTime = True
                            secondEntryFirstOnly = True
                            self.strategyLogger.info(f"{self.humanTime}, EnterOneTime TRIGGERED, Straddle dropped to half of the high: {NewStraddle:.2f} <= {checkStraddle-50:.2f}")

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")


            if secondEntryFirstOnly and lastIndexTimeData[1] in df.index and self.humanTime.time() < time(15, 15) and DayOverLimit == False and expiryTrue:
                try:
                    underlying_price = df.at[lastIndexTimeData[1], "c"]
                    call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                    data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                    data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                    if data_call_atm is not None and data_put_atm is not None:
                        avg_premium_algo_2 = (data_call_atm["c"] + data_put_atm["c"])

                        call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, (underlying_price + avg_premium_algo_2), Currentexpiry)
                        put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, (underlying_price - avg_premium_algo_2), Currentexpiry)

                        symSide_call = call_sym_atm_2[-7:-2]
                        symSide_put = put_sym_atm_2[-7:-2]

                        data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                        data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])
                        if data_call_atm_2 is not None and data_put_atm_2 is not None:
                            self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {
                                "Expiry": expiryEpoch,
                                "symSide": symSide_call,
                                "avg_premium": avg_premium_algo_2,
                                "algoName": "two"})
                            
                            self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {
                                "Expiry": expiryEpoch,
                                "symSide": symSide_put,
                                "avg_premium": avg_premium_algo_2,
                                "algoName": "two"})
                            
                            # exitTrigger = False
                            secondEntryFirstOnly = False

                except Exception as e:
                    self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

## ********************************************************************

            if expiryTrue and self.humanTime.time() < time(15, 19) and not exitTrigger_algo_3:
                if lastIndexTimeData[1] in df.index:
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        nowcheck_algo3 = (math.ceil(avg_premium_algo_3 / 100) * 100) - 100

                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium_algo_2_1_3 = (data_call_atm["c"] + data_put_atm["c"])
                            if avg_premium_algo_2_1_3 < nowcheck_algo3:
                                call_sym_atm_3 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium_algo_2_1_3, Currentexpiry, 0)
                                put_sym_atm_3 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium_algo_2_1_3, Currentexpiry, 0)
                                data_call_atm_3 = self.fetchAndCacheFnoHistData(call_sym_atm_3, lastIndexTimeData[1])
                                data_put_atm_3 = self.fetchAndCacheFnoHistData(put_sym_atm_3, lastIndexTimeData[1])

                                if data_call_atm_3 is not None and data_put_atm_3 is not None:
                                    avg_premium_algo_3 = avg_premium_algo_2_1_3
                                    exitTrigger_algo_3 = True
                                    self.strategyLogger.info(f"{self.humanTime}, exitTrigger algo two")
                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")


## ********************************************************************

            if expiryTrue and self.humanTime.time() < time(15, 19) and not exitTrigger_algo_2:
                if lastIndexTimeData[1] in df.index:
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        nowcheck_algo2 = (math.ceil(avg_premium_algo_2 / 100) * 100) - 100

                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium_algo_2_1 = (data_call_atm["c"] + data_put_atm["c"])
                            if avg_premium_algo_2_1 < nowcheck_algo2:
                                call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium_algo_2_1, Currentexpiry, 0)
                                put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium_algo_2_1, Currentexpiry, 0)
                                data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                                data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])

                                if data_call_atm_2 is not None and data_put_atm_2 is not None:
                                    avg_premium_algo_2 = avg_premium_algo_2_1
                                    exitTrigger_algo_2 = True
                                    self.strategyLogger.info(f"{self.humanTime}, exitTrigger algo two")
                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime}, Exception occurred: {e}")

            if expiryTrue and self.humanTime.time() < time(15, 19):
                if lastIndexTimeData[1] in df.index:
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        nowcheck = (math.ceil(avg_premium / 100) * 100) - 100

                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, 0)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            avg_premium1 = (data_call_atm["c"] + data_put_atm["c"])
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
                    algoTwoOpenDf = self.openPnl[self.openPnl['algoName'] == "two"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)

                    for index, row in self.openPnl.iterrows():
                        symbol = row["Symbol"]
                        symSide = symbol[-2:]
                        entry_sum = algoTwoOpenDf['EntryPrice'].sum()
                        current_sum = algoTwoOpenDf['CurrentPrice'].sum()
                        # self.strategyLogger.info(f"{self.humanTime}, FinalSl: {FinalSl}")
                        # stradddleeVaalue = (lotSize*(data_call_atm['c'] + data_put_atm['c']))
                        # self.strategyLogger.info(f"{self.humanTime}, {stradddleeVaalue}")

                        # self.closedPnl['EntryTime'] = pd.to_datetime(self.closedPnl['Key'])

                        # myClosePnl = self.closedPnl[self.closedPnl['EntryTime'].dt.date == self.humanTime.date()]
                        # self.strategyLogger.info(f"{self.humanTime} - Filtered today's closed PnL entries")

                        # realized_pnl = myClosePnl['Pnl'].sum()
                        # unrealized_pnl = self.openPnl['Pnl'].sum()
                        # total_pnl = realized_pnl + unrealized_pnl

                        # self.strategyLogger.info(f"{self.humanTime} - Realized PnL (today): {realized_pnl}, "
                        #                          f"Unrealized PnL: {unrealized_pnl}, Total PnL: {total_pnl}")

                        # if total_pnl < -FinalSl:
                        #     self.strategyLogger.info(f"{self.humanTime} - Loss limit exceeded. Exiting all open positions.")
                            
                        #     for index, row in self.openPnl.iterrows():
                        #         pnl = row['EntryPrice'] - row['CurrentPrice']
                        #         closedPnlList.append(pnl)

                        #         exitType = f"DayOver,{call_sym_atm},{put_sym_atm},{row['avg_premium']},{FinalSl}"
                        #         self.exitOrder(index, exitType, row['CurrentPrice'])

                        #         self.strategyLogger.info(
                        #             f"{self.humanTime} - Exited: {row['Symbol']}, Entry: {row['EntryPrice']}, "
                        #             f"Current: {row['CurrentPrice']}, PnL: {pnl}, ExitType: {exitType}")
                        #         DayOverLimit = True

                        try:
                            if row['algoName'] == "two":
                                if self.humanTime.time() >= time(15, 15):
                                    exitType = f"timeUp,{call_sym_atm},{put_sym_atm}, {row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if exitTrigger_algo_2 and current_sum < (0.25 * entry_sum) and (len(algoTwoOpenDf) == 2):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "two":
                                            exitType = f"exit,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                                self.strategyLogger.info(f"{self.humanTime}, {entry_sum}, {current_sum}, {(0.25 * entry_sum)}")

                                if self.openPnl is not None:
                                    algoTwoOpenDf = self.openPnl[self.openPnl['algoName'] == "two"]

                                if exitTrigger_algo_2 and (len(algoTwoOpenDf) == 1):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "two":
                                            exitType = f"exit_single_trade,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                                if (symSide == "CE" and row['Symbol'] <= call_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_call,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if (symSide == "PE" and row['Symbol'] >= put_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_put,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime} Exception occurred: {e}")


                if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                    algoOneOpenDf = self.openPnl[self.openPnl['algoName'] == "one"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)

                    for index, row in self.openPnl.iterrows():
                        symbol = row["Symbol"]
                        symSide = symbol[-2:]
                        entry_sum = algoOneOpenDf['EntryPrice'].sum()
                        current_sum = algoOneOpenDf['CurrentPrice'].sum()

                        try:
                            if row['algoName'] == "one":
                                if self.humanTime.time() >= time(15, 15):
                                    exitType = f"timeUp,{call_sym_atm},{put_sym_atm}, {row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if exitTrigger and current_sum < (0.25 * entry_sum) and (len(algoOneOpenDf) == 2):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "one":
                                            exitType = f"exit,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                                self.strategyLogger.info(f"{self.humanTime}, {entry_sum}, {current_sum}, {(0.25 * entry_sum)}")

                                if self.openPnl is not None:
                                    algoOneOpenDf = self.openPnl[self.openPnl['algoName'] == "one"]

                                if exitTrigger and (len(algoOneOpenDf) == 1):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "one":
                                            exitType = f"exit_single_trade,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                                if (symSide == "CE" and row['Symbol'] <= call_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_call,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if (symSide == "PE" and row['Symbol'] >= put_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_put,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime} Exception occurred: {e}")

                # algo Three Exit Start
                if not self.openPnl.empty and lastIndexTimeData[1] in df.index:
                    algoThreeOpenDf = self.openPnl[self.openPnl['algoName'] == "three"]

                    call_sym_atm = self.getCallSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)
                    put_sym_atm = self.getPutSym(self.timeData, baseSym, df.at[lastIndexTimeData[1], "c"], Currentexpiry)

                    for index, row in self.openPnl.iterrows():
                        symbol = row["Symbol"]
                        symSide = symbol[-2:]
                        entry_sum = algoThreeOpenDf['EntryPrice'].sum()
                        current_sum = algoThreeOpenDf['CurrentPrice'].sum()

                        try:
                            if row['algoName'] == "three":
                                if self.humanTime.time() >= time(15, 15):
                                    exitType = f"timeUp,{call_sym_atm},{put_sym_atm}, {row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if exitTrigger_algo_3 and current_sum < (0.25 * entry_sum) and (len(algoThreeOpenDf) == 2):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "three":
                                            exitType = f"exit,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                                self.strategyLogger.info(f"{self.humanTime}, {entry_sum}, {current_sum}, {(0.25 * entry_sum)}")

                                if (symSide == "CE" and row['Symbol'] <= call_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_call,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if (symSide == "PE" and row['Symbol'] >= put_sym_atm and (row['EntryPrice'] * 2) < row['CurrentPrice']):
                                    exitType = f"symSide_put,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                    self.exitOrder(index, exitType)

                                if self.openPnl is not None:
                                    algoThreeOpenDf = self.openPnl[self.openPnl['algoName'] == "three"]

                                if exitTrigger_algo_3 and (len(algoThreeOpenDf) == 1):
                                    for index, row in self.openPnl.iterrows():
                                        if row['algoName'] == "three":
                                            exitType = f"exit_single_trade,{call_sym_atm},{put_sym_atm},{row['avg_premium']}, {row['algoName']}"
                                            self.exitOrder(index, exitType, row['CurrentPrice'])

                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime} Exception occurred: {e}")


                if self.openPnl.empty:
                    algoOneEntryTrue = True
                    algoTwoEntryTrue = True

                if not self.openPnl.empty:
                    if not (self.openPnl['algoName'] == "one").any():
                        algoOneEntryTrue = True

                if not self.openPnl.empty:
                    if not (self.openPnl['algoName'] == "two").any():
                        algoTwoEntryTrue = True

                if expiryTrue and lastIndexTimeData[1] in df.index and algoOneEntryTrue and self.humanTime.time() < time(15, 15) and DayOverLimit == False:
                    if FirstEntry == False:
                        try:
                            underlying_price = df.at[lastIndexTimeData[1], "c"]
                            call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                            put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                            data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                            data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                            if data_call_atm is not None and data_put_atm is not None:
                                avg_premium = (data_call_atm["c"] + data_put_atm["c"])
                                call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium, Currentexpiry)
                                put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium, Currentexpiry)

                                symSide_call = call_sym_atm_2[-7:-2]
                                symSide_put = put_sym_atm_2[-7:-2]

                                data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                                data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])

                                if data_call_atm_2 is not None and data_put_atm_2 is not None:
                                    self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_call,
                                        "avg_premium": avg_premium,
                                        "algoName": "one"
                                    })
                                    self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_put,
                                        "avg_premium": avg_premium,
                                        "algoName": "one"
                                    })
                                    exitTrigger = False
                                    FirstEntry = True
                                    algoOneEntryTrue = False
                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")


                    if FirstEntry and exitTrigger and DayOverLimit == False:
                        try:
                            underlying_price = df.at[lastIndexTimeData[1], "c"]
                            nowcheck = (math.ceil(avg_premium / 100) * 100) - 100
                            call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                            put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                            data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                            data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                            if data_call_atm is not None and data_put_atm is not None:
                                avg_premium = (data_call_atm["c"] + data_put_atm["c"])
                                call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium, Currentexpiry)
                                put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium, Currentexpiry)

                                symSide_call = call_sym_atm_2[-7:-2]
                                symSide_put = put_sym_atm_2[-7:-2]

                                data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                                data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])

                                if data_call_atm_2 is not None and data_put_atm_2 is not None:
                                    self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_call,
                                        "avg_premium": avg_premium,
                                        "algoName": "one"
                                    })
                                    self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_put,
                                        "avg_premium": avg_premium,
                                        "algoName": "one"
                                    })
                                    exitTrigger = False
                                    algoOneEntryTrue = False
                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

                if not self.openPnl.empty:
                    if not (self.openPnl['algoName'] == "two").any():
                        algoTwoEntryTrue = True
                    else:
                        algoTwoEntryTrue = False
                else:
                    algoTwoEntryTrue = True
                if expiryTrue and lastIndexTimeData[1] in df.index and algoTwoEntryTrue and self.humanTime.time() < time(15, 15) and DayOverLimit == False:
                    if EnterOneTime and exitTrigger_algo_2:
                        try:
                            underlying_price = df.at[lastIndexTimeData[1], "c"]

                            call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                            put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                            data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                            data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                            if data_call_atm is not None and data_put_atm is not None:
                                avg_premium_algo_2 = (data_call_atm["c"] + data_put_atm["c"])
                                call_sym_atm_2 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium_algo_2, Currentexpiry)
                                put_sym_atm_2 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium_algo_2, Currentexpiry)

                                symSide_call = call_sym_atm_2[-7:-2]
                                symSide_put = put_sym_atm_2[-7:-2]

                                data_call_atm_2 = self.fetchAndCacheFnoHistData(call_sym_atm_2, lastIndexTimeData[1])
                                data_put_atm_2 = self.fetchAndCacheFnoHistData(put_sym_atm_2, lastIndexTimeData[1])

                                if data_call_atm_2 is not None and data_put_atm_2 is not None:
                                    self.entryOrder(data_call_atm_2["c"], call_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_call,
                                        "avg_premium": avg_premium_algo_2,
                                        "algoName": "two"
                                    })
                                    self.entryOrder(data_put_atm_2["c"], put_sym_atm_2, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_put,
                                        "avg_premium": avg_premium_algo_2,
                                        "algoName": "two"
                                    })
                                    self.strategyLogger.info(f"{self.humanTime}******************************")
                                    exitTrigger_algo_2 = False
                                    algoTwoEntryTrue = False
                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

## ****************************************************************************
                if not self.openPnl.empty:
                    if not (self.openPnl['algoName'] == "three").any():
                        algoThreeEntryTrue = True
                    else:
                        algoThreeEntryTrue = False
                else:
                    algoThreeEntryTrue = True
                if expiryTrue and lastIndexTimeData[1] in df.index and algoThreeEntryTrue and self.humanTime.time() < time(15, 15) and DayOverLimit == False:
                    if EnterOneTime_third and exitTrigger_algo_3:
                        try:
                            underlying_price = df.at[lastIndexTimeData[1], "c"]

                            call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                            put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                            data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                            data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                            if data_call_atm is not None and data_put_atm is not None:
                                avg_premium_algo_3 = (data_call_atm["c"] + data_put_atm["c"])
                                call_sym_atm_3 = self.getCallSym(self.timeData, baseSym, underlying_price + avg_premium_algo_3, Currentexpiry)
                                put_sym_atm_3 = self.getPutSym(self.timeData, baseSym, underlying_price - avg_premium_algo_3, Currentexpiry)

                                symSide_call = call_sym_atm_3[-7:-2]
                                symSide_put = put_sym_atm_3[-7:-2]

                                data_call_atm_3 = self.fetchAndCacheFnoHistData(call_sym_atm_3, lastIndexTimeData[1])
                                data_put_atm_3 = self.fetchAndCacheFnoHistData(put_sym_atm_3, lastIndexTimeData[1])

                                if data_call_atm_3 is not None and data_put_atm_3 is not None:
                                    self.entryOrder(data_call_atm_3["c"], call_sym_atm_3, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_call,
                                        "avg_premium": avg_premium_algo_3,
                                        "algoName": "three"
                                    })
                                    self.entryOrder(data_put_atm_3["c"], put_sym_atm_3, lotSize, "SELL", {
                                        "Expiry": expiryEpoch,
                                        "symSide": symSide_put,
                                        "avg_premium": avg_premium_algo_3,
                                        "algoName": "three"
                                    })
                                    self.strategyLogger.info(f"{self.humanTime}******************************")
                                    exitTrigger_algo_3 = False
                                    algoThreeEntryTrue = False
                        except Exception as e:
                            self.strategyLogger.info(f"{self.humanTime}Exception occurred: {e}")

        self.pnlCalculator()
        self.combinePnlCsv()
        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "PSR_123_BN_without_DSL"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 9, 20, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "BANKNIFTY"
    indexName = "NIFTY BANK"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")