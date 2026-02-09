from backtestTools.histData import getFnoBacktestData, connectToMongo
from backtestTools.algoLogic import optOverNightAlgoLogic
from backtestTools.expiry import getExpiryData
from backtestTools.util import calculate_mtm
from indicators import getFinalStrike
from datetime import datetime, time
import pandas as pd
import talib


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

        from datetime import datetime

        def append_to_txt(file_path, value):
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open(file_path, "a") as f:
                f.write(f"\n[{timestamp}] {value}")

        col = ["Target", "Stoploss", "Expiry"]
        self.addColumnsToOpenPnlDf(col)

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

        df['ema'] = talib.EMA(df['c'], timeperiod=10)
        df['prev_ema'] = df['ema'].shift(1)

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

        premiumLimitOne = 200
        premiumLimitTwo = 1000

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

                        entry = row["EntryPrice"]
                        ltp = row["CurrentPrice"]
                        current_sl = row.get("Stoploss", None)

                        # ---- Base log (always) ----
                        self.strategyLogger.info(
                            f"{self.humanTime} | CHECK TRAIL | "
                            f"EntryPrice:{entry} CurrentPrice:{ltp} ExistingSL:{current_sl}"
                        )

                        new_sl = None
                        trail_reason = None

                        # ---- Trailing conditions (SELL) ----
                        if ltp <= entry * 0.3:
                            new_sl = entry * 0.4
                            trail_reason = "LTP <= 30% of Entry → SL to 40%"

                        elif ltp <= entry * 0.4:
                            new_sl = entry * 0.5
                            trail_reason = "LTP <= 40% of Entry → SL to 50%"

                        elif ltp <= entry * 0.5:
                            new_sl = entry
                            trail_reason = "LTP <= 50% of Entry → SL to Cost"

                        else:
                            self.strategyLogger.info(
                                f"{self.humanTime} | NO TRAIL | "
                                f"LTP:{ltp} not below any trail level"
                            )

                        # ---- Apply SL only if it tightens ----
                        if new_sl is not None:

                            self.strategyLogger.info(
                                f"{self.humanTime} | TRAIL CANDIDATE | "
                                f"Reason:{trail_reason} | ProposedSL:{new_sl}"
                            )

                            if current_sl is None or pd.isna(current_sl):
                                self.openPnl.at[index, "Stoploss"] = new_sl
                                self.strategyLogger.info(
                                    f"{self.humanTime} | SL SET | NewSL:{new_sl}"
                                )

                            elif new_sl < current_sl:
                                self.openPnl.at[index, "Stoploss"] = new_sl
                                self.strategyLogger.info(
                                    f"{self.humanTime} | SL UPDATED | OldSL:{current_sl} NewSL:{new_sl}"
                                )

                            else:
                                self.strategyLogger.info(
                                    f"{self.humanTime} | SL NOT UPDATED | "
                                    f"ProposedSL:{new_sl} >= ExistingSL:{current_sl}"
                                )

                        # entry = row["EntryPrice"]
                        # ltp = row["CurrentPrice"]
                        # current_sl = row["Stoploss"]

                        # new_sl = None

                        # # Trailing milestones (SELL)
                        # if ltp <= entry * 0.5:
                        #     new_sl = entry            # Cost to cost
                        # elif ltp <= entry * 0.4:
                        #     new_sl = entry * 0.5
                        # elif ltp <= entry * 0.3:
                        #     new_sl = entry * 0.4

                        # # Apply SL only if it tightens
                        # if new_sl is not None and (pd.isna(current_sl) or new_sl < current_sl):
                        #     self.openPnl.at[index, "Stoploss"] = new_sl
                        #     self.strategyLogger.info(
                        #         f"{self.humanTime}, TRAIL SL | Entry:{entry} LTP:{ltp} NewSL:{new_sl}"
                        #     )

                        # if row['CurrentPrice'] < (row['EntryPrice'] * 0.3):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.4
                        #     self.strategyLogger.info(f"{self.humanTime}, SL1 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.4}")

                        # elif row['CurrentPrice'] < (row['EntryPrice'] * 0.4):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice'] * 0.5
                        #     self.strategyLogger.info(f"{self.humanTime},SL2 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice'] * 0.5}")

                        # elif row['CurrentPrice'] < (row['EntryPrice'] * 0.5):
                        #     self.openPnl.at[index, "Stoploss"] = row['EntryPrice']
                        #     self.strategyLogger.info(f"{self.humanTime},SL3 EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']} NewSL:{row['EntryPrice']}")
                       
                        self.strategyLogger.info(f"{self.humanTime} EntryPrice:{row['EntryPrice']} CurrentPrice:{row['CurrentPrice']}")
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

                        elif row['Stoploss'] <= row['CurrentPrice']:
                            exitType = f"TSL,{row['dayOpen']}"
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

                if putTradeCounter < 3 and candleColor == "green":

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        put_sym_atm = getFinalStrike(
                            self.timeData, lastIndexTimeData[1], baseSym, underlying_price,
                            Currentexpiry, 0, multiple, premiumLimitOne, premiumLimitTwo, "PE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger
                        )
                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_put_atm is not None:
                            self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {put_sym_atm}, entryPrice: {data_put_atm['c']}")

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")
                        append_to_txt("log.txt", self.humanTime)

                elif callTradeCounter < 3 and candleColor == "red":
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = getFinalStrike(self.timeData, lastIndexTimeData[1], baseSym, underlying_price,
                            Currentexpiry, 0, multiple, premiumLimitOne, premiumLimitTwo, "CE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger)
                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])

                        if data_call_atm is not None:

                            self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {call_sym_atm}, entryPrice: {data_call_atm['c']}")

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")
                        append_to_txt("log.txt", self.humanTime)

            if onlyOne == False and candleColor is not None and lastIndexTimeData[1] in df.index and self.humanTime.time() >= time(9, 16) and self.humanTime.time() <= time(15, 16):

                if putTradeCounter < 3 and candleColor == "red" and df.at[lastIndexTimeData[1], "ema"] > df.at[lastIndexTimeData[1], "prev_ema"]:

                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]

                        put_sym_atm = getFinalStrike(
                            self.timeData, lastIndexTimeData[1], baseSym, underlying_price,
                            Currentexpiry, 0, multiple, premiumLimitOne, premiumLimitTwo, "PE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger
                        )

                        data_put_atm = self.fetchAndCacheFnoHistData(put_sym_atm, lastIndexTimeData[1])

                        if data_put_atm is not None:
                            self.entryOrder(data_put_atm["c"], put_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {put_sym_atm}, entryPrice: {data_put_atm['c']}")
                            onlyOne = True

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in PUT branch: {e}")
                        append_to_txt("log.txt", self.humanTime)

                elif callTradeCounter < 3 and candleColor == "green": # and df.at[lastIndexTimeData[1], "ema"] < df.at[lastIndexTimeData[1], "prev_ema"]
                    try:
                        underlying_price = df.at[lastIndexTimeData[1], "c"]
                        call_sym_atm = getFinalStrike(self.timeData, lastIndexTimeData[1], baseSym, underlying_price,
                            Currentexpiry, 0, multiple, premiumLimitOne, premiumLimitTwo, "CE",
                            self.getCallSym, self.getPutSym, self.fetchAndCacheFnoHistData, self.strategyLogger)

                        data_call_atm = self.fetchAndCacheFnoHistData(call_sym_atm, lastIndexTimeData[1])
                        if data_call_atm is not None:

                            self.entryOrder(data_call_atm["c"], call_sym_atm, lotSize, "SELL", {"Expiry": expiryEpoch, "dayOpen": {df.at[lastIndexTimeData[1], 'dayopen']}})
                            self.strategyLogger.info(f"{self.humanTime}, SELL Entry order: {call_sym_atm}, entryPrice: {data_call_atm['c']}")
                            onlyOne = True

                    except Exception as e:
                        self.strategyLogger.info(f"{self.humanTime} Exception occurred in CALL branch: {e}")
                        msg = "Trade executed successfully"
                        append_to_txt("log.txt", self.humanTime)

            if self.humanTime.time() > time(15, 25):
                multiple = 100
 
        self.pnlCalculator()
        self.combinePnlCsv()

        return self.closedPnl, self.fileDir["backtestResultsStrategyUid"]


if __name__ == "__main__":
    startTime = datetime.now()

    devName = "AM"
    strategyName = "three_limit_MTMR_SS_200_1000_E10_Slope_Entry_only_call_TSL"
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