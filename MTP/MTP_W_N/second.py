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
            df_15Min = getFnoBacktestData(indexSym, startEpoch-(86400*100), endEpoch, "15Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {baseSym} in range {startDate} to {endDate}")
            raise Exception(e)


        df_15Min['callEntry'] = np.where((df_15Min['l'].shift(1) > df_15Min['c']) & (df_15Min['o'] > df_15Min['c']) & (df_15Min['o'].shift(1) > df_15Min['c'].shift(1)), "callEntry", "")
        df_15Min['callSell'] = np.where((df_15Min['callEntry'] == 'callEntry') &
            (df_15Min['callEntry'].shift(1) != 'callEntry') & (df_15Min['callEntry'].shift(2) != 'callEntry') &
            (df_15Min['callEntry'].shift(3) != 'callEntry') & (df_15Min['callEntry'].shift(4) != 'callEntry'), 'callSell', '')

        df_15Min['putEntry'] = np.where((df_15Min['l'].shift(1) < df_15Min['c']) & (df_15Min['o'] < df_15Min['c']) & (df_15Min['o'].shift(1) < df_15Min['c'].shift(1)), "putEntry", "")
        df_15Min['putSell'] = np.where((df_15Min['putEntry'] == 'putEntry') &
            (df_15Min['putEntry'].shift(1) != 'putEntry') & (df_15Min['putEntry'].shift(2) != 'putEntry') &
            (df_15Min['putEntry'].shift(3) != 'putEntry') & (df_15Min['putEntry'].shift(4) != 'putEntry'), 'putSell', '')

        df_15Min['datetime'] = pd.to_datetime(df_15Min['datetime'])
        df_15Min['date'] = df_15Min['datetime'].dt.date



        daily_hl = (df_15Min.groupby('date').agg(day_high=('h', 'max'), day_low=('l', 'min')).reset_index())

        daily_hl['prev_day_high'] = daily_hl['day_high'].shift(1)
        daily_hl['prev_day_low']  = daily_hl['day_low'].shift(1)

        df_15Min = df_15Min.merge(
            daily_hl[['date', 'prev_day_high', 'prev_day_low']],
            on='date',
            how='left')

        df_15Min[['prev_day_high', 'prev_day_low']] = (df_15Min[['prev_day_high', 'prev_day_low']].ffill())

        # df_15Min = df_15Min[df_15Min.index >= startEpoch]
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_1Min.csv")
        df_15Min.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexName}_15Min.csv")

        lastIndexTimeData = [0, 0]
        last15MinIndexTimeData = [0, 0]
        df_15Min.set_index('ti', inplace=True)

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch= expiryDatetime.timestamp()
        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        curr_day_open = None
        upPrice = None
        downPrice = None

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

            if self.humanTime.time() == time(9, 17):
                Currentexpiry = getExpiryData(self.timeData+(86400*1), baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch= expiryDatetime.timestamp()

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():

                    symSide = row["Symbol"]
                    symSide = symSide[len(symSide) - 2:]

                    # if self.humanTime.time() >= time(15, 15) and row['CurrentPrice'] < (row['EntryPrice']*0.7) and row['EntryTime'] != self.humanTime.date():
                    #     exitType = f"TimeUp"
                    #     self.exitOrder(index, exitType)

                    if self.timeData >= row["Expiry"]:
                        exitType = f"ExpiryHit"
                        self.exitOrder(index, exitType)

                    elif row["CurrentPrice"] <= row["Target"]:
                        exitType = f"TargetHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif row["CurrentPrice"] >= row["Stoploss"]:
                        exitType = f"StoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif symSide == "CE" and (timeEpochSubstract in df_15Min.index) and row['stoploss2'] < df_15Min.at[last15MinIndexTimeData[1], "c"] and row['EntryPrice'] < row['CurrentPrice']:
                        exitType = f"InitialStoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

                    elif symSide == "PE" and (timeEpochSubstract in df_15Min.index) and row['stoploss2'] > df_15Min.at[last15MinIndexTimeData[1], "c"] and row['EntryPrice'] < row['CurrentPrice']:
                        exitType = f"InitialStoplossHit"
                        self.exitOrder(index, exitType, row["CurrentPrice"])

            self.openPnl['opt_type'] = self.openPnl['Symbol'].str[-2:]

            tradecount=self.openPnl['opt_type'].value_counts()
            callCounter=tradecount.get('CE', 0)
            putCounter=tradecount.get('PE', 0)

            callPnl = self.openPnl.loc[self.openPnl['opt_type'] == 'CE', 'Pnl'].sum()
            putPnl  = self.openPnl.loc[self.openPnl['opt_type'] == 'PE', 'Pnl'].sum()

            if callCounter == 0:
                downPrice = None
            if putCounter == 0:
                upPrice = None

            if (timeEpochSubstract in df_15Min.index) and (self.humanTime.time() < time(15, 15)):
                if self.humanTime.time() == time(9, 30):
                    curr_day_open = df_15Min.at[last15MinIndexTimeData[1], "c"]

                if df_15Min.at[last15MinIndexTimeData[1], "callSell"] == "callSell" and callCounter < 2 and curr_day_open is not None and (curr_day_open > df_15Min.at[last15MinIndexTimeData[1], "c"] or df_15Min.at[last15MinIndexTimeData[1], "c"] < df_15Min.at[last15MinIndexTimeData[1], "prev_day_high"]):
                        if downPrice is not None and (downPrice-50) > df_15Min.at[last15MinIndexTimeData[1], "c"] and callPnl < 0:
                            continue

                        try:
                            callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])

                            otm = 0
                            while data["c"] > 400:
                                otm += 1
                                putSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                                data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                                otm += 1

                            otm = 0
                            while data["c"] < 100:
                                otm -= 1
                                callSym = self.getCallSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                                data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                                otm -= 1

                            target = 0.2 * data["c"]
                            stoploss = 1.5 * data["c"]
                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "stoploss2": df_15Min.at[last15MinIndexTimeData[1], "h"]})
                            downPrice = df_15Min.at[last15MinIndexTimeData[1], "c"]
                        except Exception as e:
                            self.strategyLogger.info(e)

                if df_15Min.at[last15MinIndexTimeData[1], "putSell"] == "putSell" and putCounter < 2 and curr_day_open is not None and (curr_day_open < df_15Min.at[last15MinIndexTimeData[1], "c"] or df_15Min.at[last15MinIndexTimeData[1], "c"] > df_15Min.at[last15MinIndexTimeData[1], "prev_day_low"]):
                        if upPrice is not None and (upPrice+50) < df_15Min.at[last15MinIndexTimeData[1], "c"] and putPnl < 0:
                            continue
                       
                        try:
                            callSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], expiry = Currentexpiry)
                            data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])

                            otm = 0
                            while data["c"] > 400:
                                otm += 1
                                putSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                                data = self.fetchAndCacheFnoHistData(putSym, lastIndexTimeData[1])
                                otm += 1

                            otm = 0
                            while data["c"] < 100:
                                otm -= 1
                                callSym = self.getPutSym(self.timeData, baseSym, df_15Min.at[last15MinIndexTimeData[1], "c"], Currentexpiry, otm)
                                data = self.fetchAndCacheFnoHistData(callSym, lastIndexTimeData[1])
                                otm -= 1

                            target = 0.2 * data["c"]
                            stoploss = 1.5 * data["c"]
                            
                            self.entryOrder(data["c"], callSym, lotSize, "SELL", {"Target": target, "Stoploss": stoploss, "Expiry": expiryEpoch, "stoploss2": df_15Min.at[last15MinIndexTimeData[1], "l"]})
                            upPrice = df_15Min.at[last15MinIndexTimeData[1], "c"]
                        except Exception as e:
                            self.strategyLogger.info(e)

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "second"
    version = "v1"

    startDate = datetime(2026, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 15, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    print("Calculating Daily Pnl")
    dr = calculate_mtm(closedPnl, fileDir, timeFrame="15Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime-startTime}")