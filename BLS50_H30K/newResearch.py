from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.util import calculate_mtm
from backtestTools.util import setup_logger
from termcolor import colored, cprint
from datetime import datetime
import concurrent.futures
import pandas as pd
import numpy as np
import talib
import math

class BLS01_H50(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "BLS50_H30K_rsi_7":
            raise Exception("Strategy Name Mismatch")
        cprint(f"Backtesting: {self.strategyName} UID: {self.fileDirUid}", "green")
        first_stock = portfolio if portfolio and portfolio else None
        if first_stock:
            self.backtest(first_stock, startDate, endDate)
            print(colored("Backtesting 100% complete.", "light_yellow"))
        else:
            print(colored("No stocks to backtest.", "red"))
        return self.fileDir["backtestResultsStrategyUid"], self.combinePnlCsv()

    def backtest(self, stockName, startDate, endDate):

        startTimeEpoch = startDate.timestamp()
        endTimeEpoch = endDate.timestamp()
        stockAlgoLogic = equityOverNightAlgoLogic(stockName, self.fileDir)
        logger = setup_logger(stockName, f"{self.fileDir['backtestResultsStrategyLogs']}/{stockName}.log")
        logger.propagate = False



        def convert_pinescript_to_python(df, 
                                                 ndays=5,
                                                 angle=45,
                                                 angle2=30,
                                                 scale_r=0.01,
                                                 continue_trend=True,
                                                 ma_type="SMA",
                                                 percent_threshold=55):

                    df = df.copy()
                    rad = math.pi / 180

                    # ---------------------------
                    # Trendline helper
                    # ---------------------------
                    def get_trl_val(n, base, ang):
                        return base + (n * math.tan(ang * rad) * scale_r * base)

                    # ---------------------------
                    # Moving Average
                    # ---------------------------
                    def get_ma(series, length):
                        if ma_type.upper() == "SMA":
                            return series.rolling(length).mean()
                        else:
                            return series.ewm(span=length, adjust=False).mean()

                    # ---------------------------
                    # MACD
                    # ---------------------------
                    def macd(series, fast, slow, signal=9):
                        fast_ma = series.ewm(span=fast, adjust=False).mean()
                        slow_ma = series.ewm(span=slow, adjust=False).mean()
                        macd_line = fast_ma - slow_ma
                        signal_line = macd_line.ewm(span=signal, adjust=False).mean()
                        hist = macd_line - signal_line
                        return macd_line, signal_line, hist

                    # ===========================
                    # TRENDLINE CALCULATION
                    # ===========================
                    df["base"] = df["l"].shift(ndays)

                    conds = []
                    for i in range(1, 5):
                        conds.append(
                            df["l"].shift(i) > get_trl_val(ndays - i, df["base"], angle)
                        )

                    df["con"] = conds[0] & conds[1] & conds[2] & conds[3]

                    df["signl_d_tl"] = np.where(
                        (df["l"] > get_trl_val(ndays, df["base"], angle2)) & df["con"],
                        get_trl_val(ndays, df["base"], angle2),
                        np.nan
                    )

                    # barssince logic
                    df["days_since_signl"] = df["signl_d_tl"].notna().iloc[::-1].cumsum().iloc[::-1]
                    df.loc[df["signl_d_tl"].isna(), "days_since_signl"] = np.nan

                    x_list = []
                    ab_list = []

                    for idx in range(len(df)):
                        ds = df["days_since_signl"].iloc[idx]
                        if pd.isna(ds) or ds <= 0:
                            x_list.append(np.nan)
                            ab_list.append(np.nan)
                            continue

                        ds_int = int(ds)
                        base_idx = idx - ds_int - ndays
                        if base_idx < 0:
                            x_list.append(np.nan)
                            ab_list.append(np.nan)
                            continue

                        base_low = df["l"].iloc[base_idx]

                        trl2 = get_trl_val(ds_int + ndays, base_low, angle2)
                        trl1 = get_trl_val(ds_int + ndays, base_low, angle)

                        lowest_x = df["c"].iloc[idx-ds_int:idx] - trl2
                        lowest_ab = df["c"].iloc[idx-ds_int:idx] - trl1

                        x_list.append(trl2 if lowest_x.min() > 0 else np.nan)
                        ab_list.append(trl1 if lowest_ab.min() > 0 else np.nan)

                    df["x"] = x_list
                    df["ab"] = ab_list

                    df["z"] = df["x"].isna() & df["signl_d_tl"].isna().shift(1).fillna(True)
                    df["y"] = df["signl_d_tl"].where(df["x"].isna(), df["x"])
                    df["y1"] = df["signl_d_tl"].where(df["ab"].isna(), df["ab"])

                    # ===========================
                    # MOVING AVERAGES (using c)
                    # ===========================
                    df["ma20"] = get_ma(df["c"], 20)
                    df["ma50"] = get_ma(df["c"], 50)
                    df["ma100"] = get_ma(df["c"], 100)
                    df["ma150"] = get_ma(df["c"], 150)
                    df["ma200"] = get_ma(df["c"], 200)

                    # ===========================
                    # MACD pairs
                    # ===========================
                    _, _, df["hist20_50"] = macd(df["c"], 20, 50)
                    _, _, df["hist20_100"] = macd(df["c"], 20, 100)
                    _, _, df["hist20_200"] = macd(df["c"], 20, 200)
                    _, _, df["hist50_100"] = macd(df["c"], 50, 100)
                    _, _, df["hist50_200"] = macd(df["c"], 50, 200)
                    _, _, df["hist100_200"] = macd(df["c"], 100, 200)
                    _, _, df["hist50_150"] = macd(df["c"], 50, 150)

                    # ===========================
                    # above_count
                    # ===========================
                    df["above_count"] = (
                        (df["c"] > df["ma20"]).astype(int) +
                        (df["c"] > df["ma50"]).astype(int) +
                        (df["c"] > df["ma100"]).astype(int) +
                        (df["c"] > df["ma150"]).astype(int) +
                        (df["c"] > df["ma200"]).astype(int) +
                        (df["hist20_50"] > 0).astype(int) +
                        (df["hist20_100"] > 0).astype(int) +
                        (df["hist20_200"] > 0).astype(int) +
                        (df["hist50_100"] > 0).astype(int) +
                        (df["hist50_200"] > 0).astype(int) +
                        (df["hist100_200"] > 0).astype(int) +
                        (df["hist50_150"] > 0).astype(int)
                    )

                    df["percent_buy"] = (df["above_count"] / 12) * 100
                    df["percent_sell"] = 100 - df["percent_buy"]
                    df["color"] = np.where(df["percent_buy"] > percent_threshold, "green", "red")

                    return df

        def process_stock(stock, startTimeEpoch, endTimeEpoch, df_dict):
            df = getEquityBacktestData(stock, startTimeEpoch - (86400 * 500), endTimeEpoch, "D")


            if df is not None:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df["rsi"] = talib.RSI(df["c"], timeperiod=15)

                df.dropna(inplace=True)
                df = convert_pinescript_to_python(df)
                df['trade'] = np.where((df['color'] == "green"), "trade", "")

                df.index = df.index + 33300
                df = df[df.index > startTimeEpoch]
                df_dict[stock] = df
                df.to_csv(f"{self.fileDir['backtestResultsCandleData']}{stock}_df.csv")
                print(f"Finished processing {stock}")
            else:
                print(f"Failed to fetch data for {stock}")

        def process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch):
            df_dict = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                futures = {executor.submit(process_stock, stock, startTimeEpoch, endTimeEpoch, df_dict): stock for stock in stocks}
                for future in concurrent.futures.as_completed(futures):
                    future.result()
            return df_dict

        stocks = ['AARTIIND', 'ABB', 'ABBOTINDIA', 'ABCAPITAL', 'ABFRL', 'ACC', 'ADANIENT', 'ADANIPORTS', 'AMBUJACEM', 'APOLLOHOSP', 'APOLLOTYRE', 'ASHOKLEY', 'ASIANPAINT', 'ASTRAL', 'AUROPHARMA', 'AXISBANK', 'BAJAJ-AUTO', 'BAJAJFINSV', 'BAJFINANCE', 'BALKRISIND', 'BALRAMCHIN', 'BANDHANBNK', 'BANKBARODA', 'BATAINDIA', 'BEL', 'BERGEPAINT', 'BHARATFORG', 'BHARTIARTL', 'BHEL', 'BIOCON', 'BOSCHLTD', 'BPCL', 'BRITANNIA', 'BSOFT', 'CANBK', 'CANFINHOME', 'CHAMBLFERT', 'CHOLAFIN', 'CIPLA', 'COALINDIA', 'COLPAL', 'CONCOR', 'COROMANDEL', 'CROMPTON', 'CUB', 'CUMMINSIND', 'DABUR', 'DEEPAKNTR', 'DIVISLAB', 'DIXON', 'DLF', 'DRREDDY', 'EICHERMOT', 'ESCORTS', 'EXIDEIND', 'FEDERALBNK', 'GAIL', 'GLENMARK', 'GNFC', 'GODREJCP', 'GODREJPROP', 'GRANULES', 'GRASIM', 'GUJGASLTD', 'HAVELLS', 'HCLTECH', 'HDFCAMC', 'HDFCBANK', 'HDFCLIFE', 'HEROMOTOCO', 'HINDALCO', 'HINDCOPPER', 'HINDPETRO', 'HINDUNILVR', 'HINDZINC', 'ICICIBANK', 'ICICIGI', 'ICICIPRULI', 'IDFCFIRSTB', 'IGL', 'INDHOTEL', 'INDIACEM', 'INDIGO', 'INDUSINDBK', 'INFY', 'IOC', 'IPCALAB', 'ITC', 'JINDALSTEL', 'JKCEMENT', 'JUBLFOOD', 'KOTAKBANK', 'LALPATHLAB', 'LAURUSLABS', 'LICHSGFIN', 'LT', 'LTTS', 'LUPIN', 'MANAPPURAM', 'MARICO', 'MARUTI', 'MCX', 'MFSL', 'MGL', 'MPHASIS', 'MRF', 'MUTHOOTFIN', 'NATIONALUM', 'NAUKRI', 'NESTLEIND', 'NMDC', 'NTPC', 'OBEROIRLTY', 'OFSS', 'ONGC', 'PAGEIND', 'PEL', 'PERSISTENT', 'PETRONET', 'PFC', 'PIDILITIND', 'PIIND', 'PNB', 'POWERGRID', 'RAMCOCEM', 'RBLBANK', 'RECLTD', 'RELIANCE', 'SAIL', 'SBILIFE', 'SBIN', 'SHREECEM', 'SIEMENS', 'SUNPHARMA', 'SUNTV', 'SYNGENE', 'TATACHEM', 'TATACOMM', 'TATACONSUM', 'TATAMOTORS', 'TATAPOWER', 'TATASTEEL', 'TCS', 'TECHM', 'TORNTPHARM', 'TRENT', 'TVSMOTOR', 'UBL', 'ULTRACEMCO', 'UPL', 'VEDL', 'VOLTAS', 'WIPRO']

        df_dict = process_stocks_in_parallel(stocks, startTimeEpoch, endTimeEpoch)

        amountPerTrade = 100000
        lastIndexTimeData = None
        ProfitAmount = 0
        TotalTradeCanCome = 50
        breakEven = {stock: False for stock in stocks}

        for timeData in df_dict['ADANIENT'].index:
            for stock in stocks:
                stockAlgoLogic.timeData = timeData
                stockAlgoLogic.humanTime = datetime.fromtimestamp(timeData)
                print(stock, stockAlgoLogic.humanTime)
                stock_openPnl = stockAlgoLogic.openPnl[stockAlgoLogic.openPnl['Symbol'] == stock]

                if not stock_openPnl.empty:
                    for index, row in stock_openPnl.iterrows():
                        try:
                            stockAlgoLogic.openPnl.at[index, 'CurrentPrice'] = df_dict[stock].at[lastIndexTimeData, "c"]
                        except Exception as e:
                            logger.error(f"Error updating CurrentPrice for {stock}: {e}")
                stockAlgoLogic.pnlCalculator()

                if ProfitAmount > 100000:
                    ProfitAmount = ProfitAmount - 100000
                    TotalTradeCanCome = TotalTradeCanCome + 1
                    logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome}, Datetime: {stockAlgoLogic.humanTime},ProfitAmount:-{ProfitAmount}")

                if lastIndexTimeData in df_dict[stock].index:
                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if df_dict[stock].at[lastIndexTimeData, "trade"] == "trade" and (stock_openPnl.empty) and nowTotalTrades < TotalTradeCanCome:
                        entry_price = df_dict[stock].at[lastIndexTimeData, "o"]
                        nowTotalTrades = nowTotalTrades + 1
                        breakEven[stock] = False
                        stockAlgoLogic.entryOrder(entry_price, stock, (amountPerTrade // entry_price), "BUY")
                        logger.info(f"{nowTotalTrades}, TotalTradeCanCome:-{TotalTradeCanCome}, Entry-{stock}- Datetime: {stockAlgoLogic.humanTime}, entryPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df_dict[stock].index:
                        if index in stock_openPnl.index:

                            # if df_dict[stock].at[lastIndexTimeData, "rsi"] < 30 and df_dict[stock].at[lastIndexTimeData, "c"] < row['EntryPrice']:
                            #     breakEven[stock] = True

                            # # if breakEven[stock] == True and df_dict[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                            #     exitType = "BrekevenExit"
                            #     breakEven[stock] = False
                            #     pnll = (df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice']) * row['Quantity']
                            #     stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                            #     nowTotalTrades = len(stockAlgoLogic.openPnl)
                            #     logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome},BrekevenExit- Datetime: {stockAlgoLogic.humanTime}, Stock: {stock}, pnll:{pnll}, exitPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                            if df_dict[stock].at[lastIndexTimeData, "trade"] != "trade":
                                exitType = "RsiTargetHit"
                                pnll = (df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice']) * row['Quantity']
                                ProfitAmount = ProfitAmount + pnll
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome},RsiTargetHit- Datetime: {stockAlgoLogic.humanTime}, Stock: {stock}, pnll: {pnll},ProfitAmount: {ProfitAmount}, exitPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                lastIndexTimeData = timeData
                stockAlgoLogic.pnlCalculator()

        for index, row in stockAlgoLogic.openPnl.iterrows():
            if lastIndexTimeData in df_dict[stock].index:
                if index in stockAlgoLogic.openPnl.index:
                    exitType = "TimeUp"
                    stockAlgoLogic.exitOrder(index, exitType, row['CurrentPrice'])
                    logger.info(f"Datetime: {stockAlgoLogic.humanTime}, Stock: {row['Symbol']}, exitPrice: {row['CurrentPrice']}")

if __name__ == "__main__":
    startNow = datetime.now()

    devName = "AK"
    strategyName = "BLS50_H30K_rsi_7"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 10, 30, 15, 30)

    portfolio = f'{strategyName}_combinedList'

    algoLogicObj = BLS01_H50(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculate_mtm(closedPnl, fileDir, timeFrame="15T", mtm=False, equityMarket=True)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")