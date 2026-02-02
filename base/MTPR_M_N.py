from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from indicators import addIndicatorsAndSignals, getFinalStrike
from datetime import datetime, time


class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch-(86400*10), endEpoch, "1Min")
            df_15Min = getFnoBacktestData(indexSym, startEpoch-(86400*100), endEpoch, "15Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df_15Min = addIndicatorsAndSignals(df_15Min)

        df_15Min = df_15Min[df_15Min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")

        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]

        MonthlyExpiry = getExpiryData(startEpoch, baseSym)['MonthLast']
        expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])
        oneeExpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']

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

                        if row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                            self.strategyLogger.info(f"SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.4):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.5
                            self.strategyLogger.info(f"SL2 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.5}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.6
                            self.strategyLogger.info(f"SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.6}")

                        elif row['CurrentPrice'] < (row['EntryPrice'] * 0.7):
                            self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                            self.strategyLogger.info(f"SL4 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")

                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() >= expiryDatetime.date():
                MonthlyExpiry = getExpiryData(self.timeData+86400, baseSym)['MonthLast']
                oneeExpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                print(MonthlyExpiry, oneeExpiry)
                if MonthlyExpiry == oneeExpiry:
                    MonthlyExpiry = getExpiryData(self.timeData+86400*7, baseSym)['MonthLast']

                expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if self.humanTime.time() == time(9, 30):
                oneeExpiry = getExpiryData(self.timeData+86400, baseSym)['CurrentExpiry']
                if MonthlyExpiry == oneeExpiry:
                    MonthlyExpiry = getExpiryData(self.timeData+86400*7, baseSym)['MonthLast']

            expiryDatetime = datetime.strptime(MonthlyExpiry, "%d%b%y").replace(hour=15, minute=20)
            expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    if self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

                    elif row["CurrentPrice"] <= row["Target"]:
                        exitType = f"TargetHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = f"StoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif last15MinIndexTimeData[1] in df_15Min.index:

                        if (df_15Min.at[last15MinIndexTimeData[1], "putSell"] == 1) & (symSide == "CE"):
                            exitType = "CE_exit"
                            self.exitOrder(index, exitType)

                        elif (df_15Min.at[last15MinIndexTimeData[1], "callSell"] == 1) & (symSide == "PE"):
                            exitType = "PE_exit"
                            self.exitOrder(index, exitType)

            tradecount = self.openPnl['Symbol'].str[-2:].value_counts()
            callCounter = tradecount.get('CE', 0)
            putCounter = tradecount.get('PE', 0)

            if timeEpochSubstract in df_15Min.index:
                last_idx = last15MinIndexTimeData[1]
                underlyingPrice = df_15Min.at[last_idx, "c"]
                putSellSignal = df_15Min.at[last_idx, "putSell"]
                callSellSignal = df_15Min.at[last_idx, "callSell"]

                # PUT SELL
                if putSellSignal == 1 and putCounter < 2:
                    try:
                        putSym = getFinalStrike(
                            self.timeData, lastIndexTimeData[1], last15MinIndexTimeData[1], baseSym, underlyingPrice,
                            MonthlyExpiry, 0, 100, 100, 600, "PE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger
                        )
                        if putSym is not None:
                            data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                            target = 0.2 * data["c"]
                            stoploss = 1.3 * data["c"]
                            self.entryOrder(data["c"], putSym, lotSize, "SELL",
                                            {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                    except Exception as e:
                        self.strategyLogger.info(e)

                elif callSellSignal == 1 and callCounter < 2:
                    try:
                        callSym = getFinalStrike(self.timeData, lastIndexTimeData[1], last15MinIndexTimeData[1], baseSym, underlyingPrice,
                            MonthlyExpiry, 0, 100, 100, 600, "CE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger)
                        
                        if callSym is not None:
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                            target = 0.2 * data["c"]
                            stoploss = 1.3 * data["c"]
                            self.entryOrder(data["c"], callSym, lotSize, "SELL",
                                            {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch})
                    except Exception as e:
                        self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "MTPR_M_N"
    version = "v1"

    startDate = datetime(2026, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 30, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=True, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")