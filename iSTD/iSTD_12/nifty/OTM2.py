from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.histData import getFnoBacktestData
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from datetime import datetime, time, date
import numpy as np
import talib


class algoLogic(optOverNightAlgoLogic):

    def run(self, startDate, endDate, baseSym, indexSym):

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

        startEpoch = startDate.timestamp()
        endEpoch = endDate.timestamp()

        try:
            df = getFnoBacktestData(indexSym, startEpoch, endEpoch, "1Min")
        except Exception as e:
            self.strategyLogger.info(f"Data not found for {indexSym} in range {startDate} to {endDate}")
            raise Exception(e)

        df.dropna(inplace=True)
        df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{indexSym}_1Min.csv")

        lastIndexTimeData = [0, 0]

        Currentexpiry = getExpiryData(startEpoch, baseSym)['CurrentExpiry']
        expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
        expiryEpoch = expiryDatetime.timestamp()

        lotSize = int(getExpiryData(self.timeData, baseSym)["LotSize"])

        df_CE = None
        df_PE = None
        df_straddle = None
        callSym = None
        putSym = None
        entryStraddle = None
        reFetch = False

        for timeData in df.index:

            self.timeData = float(timeData)
            self.humanTime = datetime.fromtimestamp(timeData)
            print(self.humanTime)

            if self.humanTime.time() < time(9, 16) or self.humanTime.time() > time(15, 30):
                continue

            lastIndexTimeData.pop(0)
            lastIndexTimeData.append(timeData - 60)

            if self.humanTime.time() < time(9, 16) or self.humanTime.time() > time(15, 25):
                continue

            if not self.openPnl.empty:
                for index, row in self.openPnl.iterrows():
                    try:
                        data = self.fetchAndCacheFnoHistData(row["Symbol"], lastIndexTimeData[1])
                        self.openPnl.at[index, "CurrentPrice"] = data["c"]
                    except Exception as e:
                        self.strategyLogger.info(e)
            self.pnlCalculator()

            if self.humanTime.date() > expiryDatetime.date():
                Currentexpiry = getExpiryData(self.timeData, baseSym)['CurrentExpiry']
                expiryDatetime = datetime.strptime(Currentexpiry, "%d%b%y").replace(hour=15, minute=20)
                expiryEpoch = expiryDatetime.timestamp()

            # if self.humanTime.date() < expiryDatetime.date():
            #     continue

            budget_dates = [
                date(2020, 2, 1),
                date(2021, 2, 1),
                date(2022, 2, 1),
                date(2023, 2, 1),
                date(2024, 2, 1),
                date(2024, 7, 23),
                date(2025, 2, 1)
            ]

            if self.humanTime.date() not in budget_dates:
                continue

            if (self.humanTime.time() == time(12, 1)) or (reFetch == True):
                open_epoch = lastIndexTimeData[1]

                callSym = self.getCallSym(self.timeData, baseSym, float(df.at[lastIndexTimeData[1], "c"]), Currentexpiry, 2)
                putSym = self.getPutSym(self.timeData, baseSym, float(df.at[lastIndexTimeData[1], "c"]), Currentexpiry, 2)

                df_CE = getFnoBacktestData(callSym, open_epoch - (86400 * 7), open_epoch + (86400 * 2), "1Min")
                df_PE = getFnoBacktestData(putSym, open_epoch - (86400 * 7), open_epoch + (86400 * 2), "1Min")

                self.strategyLogger.info(f"{self.humanTime} df_CE:\n{df_CE}")
                self.strategyLogger.info(f"{self.humanTime} df_PE:\n{df_PE}")

                common_index = df_PE.index.intersection(df_CE.index)
                df_PE = df_PE.loc[common_index]
                df_CE = df_CE.loc[common_index]

                df_straddle = df_PE.copy()

                for col in ['o', 'h', 'l', 'c']:
                    df_straddle[col] = df_PE[col].astype(float) + df_CE[col].astype(float)

                df_straddle['ema_10'] = talib.EMA(df_straddle['c'], 10)
                df_straddle['ema_100'] = talib.EMA(df_straddle['c'], 100)
                df_straddle['rsi_14'] = talib.RSI(df_straddle['c'], 14)

                df_straddle['Entry'] = np.where(
                    (df_straddle['ema_10'] < df_straddle['ema_100']) &
                    (df_straddle['ema_10'] < df_straddle['ema_10'].shift(1)) &
                    (df_straddle['ema_100'] < df_straddle['ema_100'].shift(1)),
                    "Entry", "")

                df_straddle['Exit'] = np.where((df_straddle['rsi_14'] > 60) &
                    (df_straddle['ema_10'] > df_straddle['ema_100']), "Exit", "")

                df_straddle.dropna(inplace=True)
                reFetch = False

                df_CE.to_csv(f"{self.fileDir['backtestResultsCandleData']}{callSym}_{self.humanTime}_1Min.csv")
                df_PE.to_csv(f"{self.fileDir['backtestResultsCandleData']}{putSym}_{self.humanTime}_1Min.csv")
                df_straddle.to_csv(f"{self.fileDir['backtestResultsCandleData']}STRADDLE_{self.humanTime}_1Min.csv")

            # ---------- EXIT ----------
            if not self.openPnl.empty and df_CE is not None and df_PE is not None and lastIndexTimeData[1] in df_straddle.index:
                for index, row in self.openPnl.iterrows():

                    if self.humanTime.time() >= time(15, 15):
                        self.exitOrder(index, "TimeUp")

                    elif df_straddle.at[lastIndexTimeData[1], "Exit"] == "Exit":
                        self.exitOrder(index, "Exit")

                    elif df_straddle.at[lastIndexTimeData[1], "c"] < (entryStraddle*0.5):
                        self.exitOrder(index, "TargetHit")
                        reFetch = True

            # ---------- STRADDLE ENTRY ----------
            if (df_straddle is not None and self.openPnl.empty and self.humanTime.time() >= time(9, 17)
                 and self.humanTime.time() < time(15, 15) and lastIndexTimeData[1] in df_straddle.index):

                if df_straddle.at[lastIndexTimeData[1], "Entry"] == "Entry":

                    entry_price = df_CE.at[lastIndexTimeData[1], "c"]
                    entryStraddle = df_straddle.at[lastIndexTimeData[1], "c"]
                    self.entryOrder(entry_price, callSym, lotSize, "SELL", {"Expiry": expiryEpoch})

                    entry_price = df_PE.at[lastIndexTimeData[1], "c"]
                    self.entryOrder(entry_price, putSym, lotSize, "SELL", {"Expiry": expiryEpoch})

        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":

    startTime = datetime.now()

    devName = "AM"
    strategyName = "iSTD_N_O2_12"
    version = "v1"

    startDate = datetime(2020, 1, 1, 9, 15)
    endDate = datetime(2026, 1, 22, 15, 30)

    algo = algoLogic(devName, strategyName, version)

    baseSym = "NIFTY"
    indexName = "NIFTY 50"

    closedPnl, fileDir = algo.run(startDate, endDate, baseSym, indexName)

    dr = calculate_mtm(closedPnl, fileDir, timeFrame="1Min", mtm=False, equityMarket=False)

    endTime = datetime.now()
    print(f"Done. Ended in {endTime - startTime}")
