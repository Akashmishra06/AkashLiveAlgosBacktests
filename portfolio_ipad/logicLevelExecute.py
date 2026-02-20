from strategyTools.statusUpdater import infoMessage, errorMessage, positionUpdator
from .connectToMongo import fetch_alpha_cumulative_pnl, fetch_alpha_margin
from datetime import datetime, time, date, timedelta
from pandas.api.types import is_datetime64_any_dtype
from strategyTools.tools import OHLCDataFetch
from configparser import ConfigParser
from time import sleep
import pandas as pd
import numpy as np
import logging
import talib
import json
import os
from tools.mediator import Mediator
import logging as strategyLogger
from typing import Set


def get_prev_day_last_accumulated_pnl(df_main: pd.DataFrame):
    """
    Returns the accumulated_pnl value from the last row
    of the previous trading day.
    """

    # Ensure date column is datetime.date
    df = df_main.copy()
    df["date"] = pd.to_datetime(df["date"]).dt.date

    # Get sorted unique dates
    unique_dates = sorted(df["date"].unique())

    if len(unique_dates) < 2:
        return None  # No previous day available

    prev_day = unique_dates[-2]

    # Filter previous day rows
    prev_day_df = df[df["date"] == prev_day]

    # Return accumulated_pnl from last row of that day
    return prev_day_df.iloc[-1]["accumulated_pnl"]

def load_holiday_dates(file_path: str) -> Set[date]:
    holiday_dates = set()

    with open(file_path, "r") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            holiday_dates.add(datetime.strptime(line, "%Y-%m-%d").date())

    return holiday_dates

def create_dir_if_not_exists(dir_path):
    if not os.path.exists(dir_path):
        os.makedirs(dir_path, exist_ok=True)

def readJson(key=None):
    file_path = f"{fileDir['jsonValue']}/data.json"
    create_dir_if_not_exists(os.path.dirname(file_path))
    try:
        with open(file_path, 'r') as json_file:
            jsonDict = json.load(json_file)
        if key:
            return jsonDict.get(key, 50)
        return jsonDict
    except (json.JSONDecodeError, IOError):
        return {}

def writeJson1(key, value):
    file_path = f"{fileDir['jsonValue']}/data.json"
    jsonDict = readJson()
    if key in jsonDict:
        print(f"Key '{key}' already exists in the JSON file. Skipping write.")
        return
    jsonDict[key] = value
    with open(file_path, 'w') as json_file:
        json.dump(jsonDict, json_file, indent=4)
        print(f"Key '{key}' added successfully.")

def writeJson(key, value):
    file_path = f"{fileDir['jsonValue']}/data.json"
    jsonDict = readJson()
    jsonDict[key] = value
    with open(file_path, 'w') as json_file:
        json.dump(jsonDict, json_file, indent=4)

def setup_logger(name, log_file, level=logging.INFO):
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
    handler = logging.FileHandler(log_file)
    handler.setFormatter(formatter)
    logger = logging.getLogger(name)
    logger.setLevel(level)
    logger.addHandler(handler)
    logging.basicConfig(level=level, filemode='a', force=True)
    return logger

def combineClosePnlCSV():
    closeCsvDir = fileDir["closedPositions"]
    if not os.listdir(closeCsvDir):
        return
    csvFiles = [file for file in os.listdir(closeCsvDir) if file.endswith(".csv")]
    closedPnl = pd.concat([pd.read_csv(os.path.join(closeCsvDir, file)) for file in csvFiles])
    if closedPnl.empty:
        return None
    if not is_datetime64_any_dtype(closedPnl["Key"]):
        closedPnl["Key"] = pd.to_datetime(closedPnl["Key"])
    if not is_datetime64_any_dtype(closedPnl["ExitTime"]):
        closedPnl["ExitTime"] = pd.to_datetime(closedPnl["ExitTime"])
    if "Unnamed: 0" in closedPnl.columns:
        closedPnl.drop(columns=["Unnamed: 0"], inplace=True)
    closedPnl.sort_values(by=["Key"], inplace=True)
    closedPnl.reset_index(inplace=True, drop=True)
    closedPnl.to_csv(f"{fileDir['baseJson']}/closePnl.csv", index=False)
    closedPnl[closedPnl["ExitTime"].dt.date == datetime.now().date()].to_csv(f"{fileDir['closedPositionsLogs']}/closePnl_{datetime.now().date().__str__()}.csv", index=False)
    return closedPnl

def combineOpenPnlCSV():
    openCsvDir = fileDir["openPositions"]
    if not os.listdir(openCsvDir):
        return pd.DataFrame()
    csvFiles = [file for file in os.listdir(openCsvDir) if file.endswith(".csv")]
    if not csvFiles:
        return pd.DataFrame()
    data_frames = []
    for file in csvFiles:
        file_path = os.path.join(openCsvDir, file)
        if os.stat(file_path).st_size == 0:
            continue
        try:
            df = pd.read_csv(file_path)
            if df.empty:
                continue
            data_frames.append(df)
        except pd.errors.EmptyDataError:
            print(f"Error: No columns in {file_path}")
        except Exception as e:
            print(f"Error reading {file_path}: {str(e)}")
    if not data_frames:
        return pd.DataFrame()
    openPnl = pd.concat(data_frames, ignore_index=True)
    if "EntryTime" in openPnl.columns and not is_datetime64_any_dtype(openPnl["EntryTime"]):
        openPnl["EntryTime"] = pd.to_datetime(openPnl["EntryTime"], errors="coerce")
    if "Unnamed: 0" in openPnl.columns:
        openPnl.drop(columns=["Unnamed: 0"], inplace=True)
    openPnl.sort_values(by=["EntryTime"], inplace=True)
    openPnl.reset_index(inplace=True, drop=True)
    openPnl.to_csv(f"{fileDir['baseJson']}/openPnl.csv", index=False)
    openPnl[openPnl["EntryTime"].dt.date == datetime.now().date()].to_csv(f"{fileDir['openPositionsLogs']}/openPnl_{datetime.now().date().__str__()}.csv", index=False)
    return openPnl


class baseSymbolClass:
    def __init__(self, symbol):

        self.symbol = symbol
        self.realizedPnl = 0
        self.unrealizedPnl = 0
        self.netPnl = 0

        self.openPnl = pd.DataFrame(columns=["EntryTime", "Symbol", "EntryPrice", "CurrentPrice", "Quantity", "PositionStatus", "Pnl", "Expiry"])
        self.closedPnl = pd.DataFrame(columns=["Key", "ExitTime", "Symbol", "EntryPrice", "ExitPrice", "Quantity", "PositionStatus", "Pnl", "ExitType"])
        self.addColumnsToOpenPnlDf(["Expiry"])

        stockLogDir = f"{fileDir['stockLogs']}/{self.symbol}"
        os.makedirs(stockLogDir, exist_ok=True)
        self.stockLogger = setup_logger(self.symbol, f"{stockLogDir}/log_{datetime.now().replace(microsecond=0)}.log")
        self.stockLogger.propagate = False

        self.readOpenPnlCsv()
        self.readClosePnlCsv()

    def readOpenPnlCsv(self):
        openPnlCsvFilePath = f"{fileDir['openPositions']}/{self.symbol}_openPositions.csv"
        if os.path.exists(openPnlCsvFilePath):
            openPnlCsvDf = pd.read_csv(openPnlCsvFilePath)
            if 'Unnamed: 0' in openPnlCsvDf.columns:
                openPnlCsvDf.drop(columns=['Unnamed: 0'], inplace=True)
            self.openPnl = pd.concat([self.openPnl, openPnlCsvDf])
            if not is_datetime64_any_dtype(self.openPnl["EntryTime"]):
                self.openPnl["EntryTime"] = pd.to_datetime(self.openPnl["EntryTime"])
            if not is_datetime64_any_dtype(self.openPnl["Expiry"]):
                self.openPnl["Expiry"] = pd.to_datetime(self.openPnl["Expiry"])
            self.stockLogger.info("OpenPnl CSV read successfully.")
        else:
            self.stockLogger.info("OpenPnl CSV not found.")

    def writeOpenPnlCsv(self):
        self.openPnl.to_csv(f"{fileDir['openPositions']}/{self.symbol}_openPositions.csv")

    def readClosePnlCsv(self):
        closePnlCsvFilePath = f"{fileDir['closedPositions']}/{self.symbol}_closedPositions.csv"
        if os.path.exists(closePnlCsvFilePath):
            closePnlCsvDf = pd.read_csv(closePnlCsvFilePath)
            if 'Unnamed: 0' in closePnlCsvDf.columns:
                closePnlCsvDf.drop(columns=['Unnamed: 0'], inplace=True)
            self.closedPnl = pd.concat([self.closedPnl, closePnlCsvDf])
            if not is_datetime64_any_dtype(self.closedPnl["Key"]):
                self.closedPnl["Key"] = pd.to_datetime(self.closedPnl["Key"])
            if not is_datetime64_any_dtype(self.closedPnl["ExitTime"]):
                self.closedPnl["ExitTime"] = pd.to_datetime(
                    self.closedPnl["ExitTime"])
            self.stockLogger.info("ClosedPnl CSV read successfully.")
        else:
            self.stockLogger.info("ClosedPnl CSV not found.")

    def writeClosePnlCsv(self):
        self.closedPnl.to_csv(f"{fileDir['closedPositions']}/{self.symbol}_closedPositions.csv")

    def entryOrder(self, symbol, entryPrice, quantity, orderSide, extraCols=None):

        newTrade = pd.DataFrame({
            "EntryTime": datetime.now(),
            "Symbol": symbol,
            "EntryPrice": entryPrice,
            "CurrentPrice": entryPrice,
            "Quantity": quantity,
            "PositionStatus": 1 if orderSide == "BUY" else -1,
            "Pnl": 0}, index=[0])

        if extraCols:
            for key in extraCols.keys():
                newTrade[key] = extraCols[key]
        self.openPnl = pd.concat([self.openPnl, newTrade], ignore_index=True)
        self.openPnl.reset_index(inplace=True, drop=True)

        self.stockLogger.info(f'Entry {orderSide}: {symbol} @ {entryPrice} ({len(self.openPnl[self.openPnl["PositionStatus"] == -1])})'.upper())
        self.writeOpenPnlCsv()

    def exitOrder(self, index, exitPrice, exitType):
        trade_to_close = self.openPnl.loc[index].to_dict()

        self.openPnl.drop(index=index, inplace=True)
        trade_to_close['Key'] = trade_to_close['EntryTime']
        trade_to_close['ExitTime'] = datetime.now()
        trade_to_close['ExitPrice'] = exitPrice
        trade_to_close['Pnl'] = (trade_to_close['ExitPrice'] - trade_to_close['EntryPrice']) * trade_to_close['Quantity'] * trade_to_close['PositionStatus']
        trade_to_close['ExitType'] = exitType
        for col in self.openPnl.columns:
            if col not in self.closedPnl.columns:
                del trade_to_close[col]
        self.closedPnl = pd.concat([self.closedPnl, pd.DataFrame([trade_to_close])], ignore_index=True)
        self.closedPnl.reset_index(inplace=True, drop=True)
        percentPnl = round(((trade_to_close['ExitPrice'] - trade_to_close['EntryPrice'])*trade_to_close['PositionStatus'])*100/trade_to_close['EntryPrice'], 1)
        percentPnl = "+" + \
            str(percentPnl) if percentPnl > 0 else "-" + str(abs(percentPnl))
        self.writeOpenPnlCsv()
        self.writeClosePnlCsv()
        self.stockLogger.info(f'Exit {exitType}: {trade_to_close["Symbol"]} @ {exitPrice}'.upper())

    def pnlCalculator(self):
        if not self.openPnl.empty:
            self.openPnl["Pnl"] = (self.openPnl["CurrentPrice"] - self.openPnl["EntryPrice"]) * self.openPnl["Quantity"] * self.openPnl["PositionStatus"]
            self.unrealizedPnl = self.openPnl["Pnl"].sum()
            self.writeOpenPnlCsv()
        else:
            self.unrealizedPnl = 0
        if not self.closedPnl.empty:
            self.realizedPnl = self.closedPnl["Pnl"].sum()
        else:
            self.realizedPnl = 0
        self.netPnl = self.unrealizedPnl + self.realizedPnl
        self.openPnl["EntryTime"] = pd.to_datetime(self.openPnl["EntryTime"])
        self.closedPnl["Key"] = pd.to_datetime(self.closedPnl["Key"])
        self.closedPnl["ExitTime"] = pd.to_datetime(self.closedPnl["ExitTime"])

    def addColumnsToOpenPnlDf(self, columns):
        for col in columns:
            self.openPnl[col] = None


class Strategy:
    def __init__(self, clientID):
        self.candle_1Min = {}
        self.mediator = Mediator('portfolio_ipad')
        self.clientID = clientID
        self.baseSymObj = None

        # Instance variables instead of globals
        self.config = ConfigParser()

        self.config.read('/root/Executor_RMS/logics/portfolio_ipad/config.ini')
        

        self.algoName = f"iP_{clientID}"
        global algoName
        algoName = self.algoName
        
        # Setup directory structure
        base_dir = os.getcwd()
        state_dir = os.path.join(base_dir, "state")
        portfolio_dir = os.path.join(state_dir, "portfolio_iPad")
        algo_base_dir = os.path.join(portfolio_dir, self.algoName)

        os.makedirs(algo_base_dir, exist_ok=True)

        logFileFolder = algo_base_dir
        jsonFileFolder = algo_base_dir

        self.fileDir = {
            "baseJson": f"{jsonFileFolder}/json",
            "openPositions": f"{jsonFileFolder}/json/OpenPositions",
            "closedPositions": f"{jsonFileFolder}/json/ClosedPositions",
            "baseLog": f"{logFileFolder}/logs",
            "strategyLogs": f"{logFileFolder}/logs/StrategyLog",
            "stockLogs": f"{logFileFolder}/logs/StrategyLog/Stocks",
            "jsonValue": f"{jsonFileFolder}/jsonss/jsonFiles",
            "openPositionsLogs": f"{logFileFolder}/StrategyLog/OpenPositions",
            "closedPositionsLogs": f"{logFileFolder}/StrategyLog/ClosePositions",
        }
        global fileDir
        fileDir = self.fileDir
        for keyDir in self.fileDir.keys():
            os.makedirs(self.fileDir[keyDir], exist_ok=True)

        baseSym = self.config.get('strategyParameters', 'baseSym')
        strategyLogger.info(f"RUNNING STRATEGY ON THE FOLLOWING BASE SYMBOL: {baseSym}")

        self.symMap = {"NIFTY": "NIFTY 50"}

    def updateOpenPositionsInfra(self):
        """Update open positions infrastructure"""
        if self.baseSymObj is None:
            return
            
        combinedOpenPnl = pd.DataFrame(columns=[
            "EntryTime", "Symbol", "EntryPrice", "CurrentPrice", 
            "Quantity", "PositionStatus", "Pnl", "Expiry"
        ])
        combinedOpenPnl = pd.concat([combinedOpenPnl, self.baseSymObj.openPnl], ignore_index=True)
        combinedOpenPnl['EntryTime'] = combinedOpenPnl['EntryTime'].astype(str)
        positionUpdator(combinedOpenPnl, 'Process 1', self.algoName)

    def run_strategy(self, baseSym, clientID):
        """Main strategy execution loop"""
        try:
            self.baseSymObj = baseSymbolClass(baseSym)
            self.candle_1Min = {'last_candle_time': 0, 'df': None}

            writeJson("algoStart", True)
            writeJson1(f"DSL{clientID}", False)
            writeJson1(f"DSL_date{clientID}", None)

            HOLIDAY_DATES = load_holiday_dates("/root/Executor_RMS/logics/portfolio_ipad/marketHoliday.md")
            clientMargin = fetch_alpha_margin(clientID)

            while True:
                self.exec_strategy(baseSym, clientID, clientMargin, HOLIDAY_DATES)
                sleep(0.5)
                self.updateOpenPositionsInfra()
                combineClosePnlCSV()
                combineOpenPnlCSV()
                
        except Exception as err:
            errorMessage(algoName=self.algoName, message=str(err))
            strategyLogger.exception(str(err))

    def getmtm(self):
        """Calculate mark-to-market PnL"""
        if self.baseSymObj is None:
            return 0
            
        self.baseSymObj.closedPnl['ExitTime'] = pd.to_datetime(self.baseSymObj.closedPnl['ExitTime'])
        currentDayClosedPnl = self.baseSymObj.closedPnl[
            self.baseSymObj.closedPnl['ExitTime'].dt.date == datetime.now().date()
        ]
        mtmPnl = int(self.baseSymObj.openPnl['Pnl'].sum()) + int(currentDayClosedPnl['Pnl'].sum())
        return mtmPnl

    def exec_strategy(self, baseSym, clientID, clientMargin, HOLIDAY_DATES):
        """Execute strategy logic"""
        try:
            # Check algo start flag
            algoStart = readJson("algoStart")
            if algoStart:

                if not self.baseSymObj.openPnl.empty:
                    self.mediator.entry(clientID)
                if self.baseSymObj.openPnl.empty:
                    self.mediator.exit(clientID)

                margin = fetch_alpha_margin(clientID)
                writeJson("algoStart", False)

                if not margin:
                    return

                mtmPnl = self.getmtm()
                pnl_pct = round((mtmPnl / margin) * 100, 2)
                dsl = int(margin * 0.01)

                infoMessage(
                    algoName=self.algoName,
                    message=(
                        f"PnL: {int(mtmPnl):,} [{pnl_pct}%], "
                        f"margin: {int(margin):,}, "
                        f"DSL: {dsl:,} [1%]"
                    )
                )

            currentDatetime = datetime.now()

            # Check trading hours
            if currentDatetime.time() <= time(9, 16) or currentDatetime.time() > time(15, 29):
                return

            self.baseSymObj.pnlCalculator()

            # Fetch candle data
            self.candle_1Min['df'], candle_flag_1Min, self.candle_1Min['last_candle_time'] = OHLCDataFetch(
                self.symMap[baseSym], 
                currentDatetime.timestamp(), 
                self.candle_1Min['last_candle_time'], 
                1, 
                10, 
                self.candle_1Min['df'], 
                self.baseSymObj.stockLogger
            )

            if not candle_flag_1Min or self.candle_1Min['df'] is None:
                return

            # Fetch and process data
            df_main = fetch_alpha_cumulative_pnl(clientID)
            df_main["rsi"] = talib.RSI(df_main["accumulated_pnl"], 14)
            df_main["ema10"] = talib.EMA(df_main["accumulated_pnl"], 10)
            df_main["ema100"] = talib.EMA(df_main["accumulated_pnl"], 100)

            df_main["prev_ema10"] = df_main["ema10"].shift(1)
            df_main["prev_ema100"] = df_main["ema100"].shift(1)

            df_main = df_main[df_main["ema100"].notna()]

            lastRunMinutesecond = -1
            currentMinute = currentDatetime.minute
            if currentMinute % 15 == 0 and lastRunMinutesecond != currentMinute:
                mtmPnl = self.getmtm()
                infoMessage(algoName=algoName, message=f"MTM Pnl: {mtmPnl}")
                lastRunMinutesecond = currentMinute
            elif currentMinute % 15 != 0:
                lastRunMinutesecond = -1

            # Generate signals
            df_main["entry_signal"] = np.where(
                (df_main["ema10"] > df_main["prev_ema10"]) &
                (df_main["ema100"] > df_main["prev_ema100"]) & 
                (df_main["ema10"] > df_main["ema100"]), 
                "entry_signal", 
                ""
            )
            df_main["exit_signal"] = np.where(
                (df_main["ema10"] < df_main["ema100"]) & 
                (df_main["rsi"] < 30), 
                "exit_signal", 
                ""
            )

            df_main = df_main.round(2)
            latest = df_main.iloc[-1]

            df_main.to_csv("main.csv")
            prev_day_pnl = get_prev_day_last_accumulated_pnl(df_main)

            # Get current price
            try:
                currentPrice = latest['accumulated_pnl']
                self.baseSymObj.stockLogger.info(f'[Tick] => Current Price: {currentPrice}')
            except Exception as e:
                return

            # Update open positions with current price
            if not self.baseSymObj.openPnl.empty:
                for index, row in self.baseSymObj.openPnl.iterrows():
                    optionSymbol = row["Symbol"]
                    try:
                        self.baseSymObj.openPnl.at[index, "CurrentPrice"] = currentPrice
                        self.baseSymObj.stockLogger.info(
                            f'Open Position: {optionSymbol} Current Price: {currentPrice}'
                        )
                    except Exception:
                        self.baseSymObj.stockLogger.warning(
                            f"Open Position: {optionSymbol} Current Price: Couldn't Fetch"
                        )

            self.baseSymObj.pnlCalculator()

            # ---------------------------------------------- Exit Conditions ----------------------------------------------
            if not self.baseSymObj.openPnl.empty:
                for index, row in self.baseSymObj.openPnl.iterrows():
                    try:
                        # Exit signal condition
                        if latest['exit_signal'] == "exit_signal":
                            exitType = "exit_signal"
                            log_msg = (
                                f"{currentDatetime} exit_signal | "
                                f"EXIT | {clientID} @ {currentPrice} | "
                                f"Type: {exitType} "
                                f"(E10: {latest['ema10']}, "
                                f"E100: {latest['ema100']}, "
                                f"R: {latest['rsi']})"
                            )
                            self.baseSymObj.stockLogger.info(log_msg)
                            self.baseSymObj.exitOrder(index, currentPrice, exitType)
                            self.mediator.exit(clientID)
                            infoMessage(algoName=self.algoName, message=log_msg)
                            continue

                        # Check for next day holiday/weekend
                        next_date = currentDatetime.date() + timedelta(days=1)
                        is_holiday = next_date in HOLIDAY_DATES
                        is_weekend = next_date.weekday() in (5, 6)

                        if (is_holiday or is_weekend) and currentDatetime.time() >= time(15, 20):
                            exitType = "NextDay_Holiday"
                            log_msg = (
                                f"{currentDatetime} NextDay_Holiday | "
                                f"EXIT | {clientID} @ {currentPrice} | "
                                f"Type: {exitType} "
                                f"(E10: {latest['ema10']}, "
                                f"E100: {latest['ema100']}, "
                                f"R: {latest['rsi']})"
                            )
                            self.baseSymObj.stockLogger.info(log_msg)
                            self.baseSymObj.exitOrder(index, currentPrice, exitType)
                            self.mediator.exit(clientID)
                            infoMessage(algoName=self.algoName, message=log_msg)
                            continue

                        # Stoploss condition
                        if (prev_day_pnl-dsl) >= currentPrice:
                            exitType = "StoplossHit"
                            log_msg = (
                                f"{currentDatetime} StoplossHit | "
                                f"EXIT | {clientID} @ {currentPrice} | "
                                f"Type: {exitType} "
                                f"(E10: {latest['ema10']}, "
                                f"E100: {latest['ema100']}, "
                                f"R: {latest['rsi']})"
                            )
                            self.baseSymObj.stockLogger.info(log_msg)
                            self.baseSymObj.exitOrder(index, currentPrice, exitType)
                            self.mediator.exit(clientID)
                            infoMessage(algoName=self.algoName, message=log_msg)
                            writeJson(f"DSL{clientID}", True)

                    except Exception as e:
                        strategyLogger.warning(f"ExitTime Error {baseSym}, {e}")
                        continue

            # ---------------------------------------------- Entry Conditions ----------------------------------------------
            DSL = readJson(f"DSL{clientID}")
            DSL_date = readJson(f"DSL_date{clientID}")

            today_str = currentDatetime.date().isoformat()
            current_time = currentDatetime.time()

            # Reset DSL flag if new day
            if DSL_date is not None and DSL_date != today_str:
                writeJson(f"DSL{clientID}", False)
                DSL = False

            is_trading_time = time(9, 17) <= current_time < time(15, 15)
            is_new_day = DSL_date != today_str
            no_open_position = self.baseSymObj.openPnl.empty
            entry_signal = latest.get("entry_signal") == "entry_signal"

            # Entry logic
            if (not DSL and is_new_day and is_trading_time and no_open_position and entry_signal):
                self.baseSymObj.entryOrder(clientID, currentPrice, 1, "BUY")

                log_msg = (
                    f"{currentDatetime} entry_signal | "
                    f"ENTRY | iP_{clientID} @ {currentPrice} "
                    f"(E10: {latest['ema10']}, "
                    f"E100: {latest['ema100']}, "
                    f"R: {latest['rsi']})"
                )
                self.baseSymObj.stockLogger.info(log_msg)
                self.mediator.entry(clientID)
                infoMessage(algoName=self.algoName, message=log_msg)

            self.baseSymObj.pnlCalculator()
            sleep(0.1)

        except Exception as err:
            errorMessage(algoName=self.algoName, message=str(err))
            strategyLogger.exception(str(err))
