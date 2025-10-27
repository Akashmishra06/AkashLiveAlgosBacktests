from backtestTools.algoLogic import baseAlgoLogic, equityOverNightAlgoLogic
from backtestTools.histData import getEquityBacktestData
from backtestTools.util import calculate_mtm
from backtestTools.util import setup_logger
from termcolor import colored, cprint
from datetime import datetime
import concurrent.futures
import pandas as pd
import talib


class BLS01_H50(baseAlgoLogic):
    def runBacktest(self, portfolio, startDate, endDate):
        if self.strategyName != "BLS01_H50":
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

        def process_stock(stock, startTimeEpoch, endTimeEpoch, df_dict):
            df = getEquityBacktestData(stock, startTimeEpoch - (86400 * 500), endTimeEpoch, "D")

            if df is not None:
                df['datetime'] = pd.to_datetime(df['datetime'])
                df["rsi"] = talib.RSI(df["c"], timeperiod=7)
                df.dropna(inplace=True)
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

                for index, row in stock_openPnl.iterrows():
                    if lastIndexTimeData in df_dict[stock].index:
                        if index in stock_openPnl.index:

                            if df_dict[stock].at[lastIndexTimeData, "rsi"] < 30 and df_dict[stock].at[lastIndexTimeData, "c"] < row['EntryPrice']:
                                breakEven[stock] = True

                            if breakEven[stock] == True and df_dict[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "BrekevenExit"
                                breakEven[stock] = False
                                pnll = (df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice']) * row['Quantity']
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome},BrekevenExit- Datetime: {stockAlgoLogic.humanTime}, Stock: {stock}, pnll:{pnll}, exitPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                            elif df_dict[stock].at[lastIndexTimeData, "rsi"] < 30 and df_dict[stock].at[lastIndexTimeData, "c"] > row['EntryPrice']:
                                exitType = "RsiTargetHit"
                                pnll = (df_dict[stock].at[lastIndexTimeData, "c"] - row['EntryPrice']) * row['Quantity']
                                ProfitAmount = ProfitAmount + pnll
                                stockAlgoLogic.exitOrder(index, exitType, df_dict[stock].at[lastIndexTimeData, "c"])
                                nowTotalTrades = len(stockAlgoLogic.openPnl)
                                logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome},RsiTargetHit- Datetime: {stockAlgoLogic.humanTime}, Stock: {stock}, pnll: {pnll},ProfitAmount: {ProfitAmount}, exitPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

                if ProfitAmount > 100000:
                    ProfitAmount = ProfitAmount - 100000
                    TotalTradeCanCome = TotalTradeCanCome + 1
                    logger.info(f"{nowTotalTrades}, TotalTradeCanCome:- {TotalTradeCanCome}, Datetime: {stockAlgoLogic.humanTime},ProfitAmount:-{ProfitAmount}")

                if lastIndexTimeData in df_dict[stock].index:
                    nowTotalTrades = len(stockAlgoLogic.openPnl)
                    if df_dict[stock].at[lastIndexTimeData, "rsi"] > 60 and (stock_openPnl.empty) and nowTotalTrades < TotalTradeCanCome:
                        entry_price = df_dict[stock].at[lastIndexTimeData, "c"]
                        nowTotalTrades = nowTotalTrades + 1
                        breakEven[stock] = False
                        stockAlgoLogic.entryOrder(entry_price, stock, (amountPerTrade // entry_price), "BUY")
                        logger.info(f"{nowTotalTrades}, TotalTradeCanCome:-{TotalTradeCanCome}, Entry-{stock}- Datetime: {stockAlgoLogic.humanTime}, entryPrice: {df_dict[stock].at[lastIndexTimeData, 'c']}")

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
    strategyName = "BLS01_H50"
    version = "v1"

    startDate = datetime(2020, 4, 1, 9, 15)
    endDate = datetime(2025, 10, 30, 15, 30)

    portfolio = f'{strategyName}_combinedList'

    algoLogicObj = BLS01_H50(devName, strategyName, version)
    fileDir, closedPnl = algoLogicObj.runBacktest(portfolio, startDate, endDate)

    dailyReport = calculate_mtm(closedPnl, fileDir, timeFrame="15T", mtm=False, equityMarket=True)

    endNow = datetime.now()
    print(f"Done. Ended in {endNow-startNow}")