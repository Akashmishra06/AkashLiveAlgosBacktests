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
            df_2Min = getFnoBacktestData(indexSym, startEpoch-(86400*100), endEpoch, "2Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df_2Min['rsi'] = talib.RSI(df_2Min['c'], timeperiod=14)
        df_2Min['callSell'] = np.where((df_2Min['rsi'] < 50) & (df_2Min['c'] < df_2Min['o']) & ((df_2Min['rsi'] > 30)), "callSell", "")
        df_2Min['putSell'] = np.where((df_2Min['rsi'] > 50) & (df_2Min['c'] > df_2Min['o']) & ((df_2Min['rsi'] < 70)), "putSell", "")

        df_2Min['callExit'] = np.where((df_2Min['rsi'] > 70) & (df_2Min['rsi'].shift(1) > 70) & (df_2Min['c'] > df_2Min['o']), "callExit", "")
        df_2Min['putExit'] = np.where((df_2Min['rsi'] < 30) & (df_2Min['rsi'].shift(1) < 30) & (df_2Min['c'] < df_2Min['o']), "putExit", "")

        df_2Min = df_2Min[df_2Min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_2Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")

        lastIndexTimeData = [0, 0]
        last2MinIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        for timeData in df.index: 

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 30)):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData-60)
            timeEpochSubstract = (timeData-120)
            if timeEpochSubstract in df_2Min.index:
                last2MinIndexTimeData.pop(0)
                last2MinIndexTimeData.append(timeEpochSubstract)

            if (self.humanTime.time() < time(9, 16)) | (self.humanTime.time() > time(15, 25)):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]

                        if row['CurrentPrice'] < (row['EntryPrice'] * 0.2):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.3
                            self.strategyLogger.info(f"SL0 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.3}")
                        
                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.4):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.5
                            self.strategyLogger.info(f"SL2 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.5}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                            self.strategyLogger.info(f"SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")

                    except Exception as e:
                        self.strategyLogger.info(e)
                self.strategyLogger.info(f"{self.humanTime} | current strangle: {self.openPnl['CurrentPrice'].sum()} | entry strangle: {self.openPnl['EntryPrice'].sum()}")
            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData + 86400, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if self.humanTime.time() >= time(15, 15):
                        exitType = f"timeUp"
                        self.exitOrder(index, exitType)

                    elif row['Stoploss'] < row['CurrentPrice']:
                        exitType = f"TSL"
                        self.exitOrder(index, exitType)

                    elif (row['EntryPrice'] * 2) < row['CurrentPrice']:
                        exitType = f"Stoploss"
                        self.exitOrder(index, exitType)

                    elif (row['EntryPrice'] * 0.1) > row['CurrentPrice']:
                        exitType = f"Target"
                        self.exitOrder(index, exitType)

                    elif last2MinIndexTimeData[1] in df_2Min.index:

                        if symSide == "CE" and df_2Min.at[last2MinIndexTimeData[1], "callExit"] == "callExit" and row['EntryPrice'] < row['CurrentPrice']:
                            exitType = f"callRsiExit"
                            self.exitOrder(index, exitType)

                        elif symSide == "PE" and df_2Min.at[last2MinIndexTimeData[1], "putExit"] == "putExit" and row['EntryPrice'] < row['CurrentPrice']:
                            exitType = f"putRsiExit"
                            self.exitOrder(index, exitType)

            putTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('PE', 0)
            callTradeCounter = self.openPnl['Symbol'].str[-2:].value_counts().get('CE', 0)

            if (timeEpochSubstract in df_2Min.index) and self.humanTime.time() < time(15, 15) and self.humanTime.time() >= time(9, 16):

                if df_2Min.at[last2MinIndexTimeData[1], "putSell"] == "putSell" and putTradeCounter == 0:

                    try:
                        underlying_price = df_2Min.at[last2MinIndexTimeData[1], "c"]
                        
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        
                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:
                            taking_price = (data_call_atm["c"] + data_put_atm["c"]) * 0.1
                            otm = 0

                            while otm <= 50:
                                try:
                                    putSym = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry, otm)
                                    data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                                except Exception as e:
                                    self.strategyLogger.info(e)
                                    self.strategyLogger.info(f"Data not found for {putSym}")
                                    otm += 1
                                    continue

                                if data is not None and data["c"] < taking_price:
                                    self.entryOrder(data["c"], putSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                                    self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {putSym}, entryPrice: {data['c']}, taking_price: {taking_price}")
                                    break

                                otm += 1

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")

                elif df_2Min.at[last2MinIndexTimeData[1], "callSell"] == "callSell" and callTradeCounter == 0:
                    try:
                        underlying_price = df_2Min.at[last2MinIndexTimeData[1], "c"]
                        call_sym_atm = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry)
                        put_sym_atm = self.getPutSym(self.timeData, baseSym, underlying_price, Currentexpiry)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None and data_put_atm is not None:

                            taking_price = (data_call_atm["c"] + data_put_atm["c"]) * 0.1
                            otm = 0

                            while otm <= 50:
                                try:
                                    callSym = self.getCallSym(self.timeData, baseSym, underlying_price, Currentexpiry, otm)
                                    data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                                except Exception as e:
                                    self.strategyLogger.info(e)
                                    self.strategyLogger.info(f"Data not found for {callSym}")
                                    otm += 1
                                    continue

                                if data is not None and data["c"] < taking_price:
                                    self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Expiry": expiryEpoch})
                                    self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {callSym}, entryPrice: {data['c']}, taking_price: {taking_price}")
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
    strategyName = "RMA_N_2Min"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 12, 31, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    dr = calculate_mtm(closedPnl, fileDir, timeFrame="1Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")