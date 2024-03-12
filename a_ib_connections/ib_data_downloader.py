import os

from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.contract import *
import pandas as pd
from datetime import datetime, timedelta


class TestApp(EClient, EWrapper):
    def __init__(self, udl_code, exc=None, req_type='data'):
        EClient.__init__(self, self)
        self.results = list()
        self.exchange = exc
        self.underlying_code = udl_code
        self.req_type = req_type
        self.last_trade_date_or_contract_month = None

    def set_req_type(self, req_type):
        self.req_type = req_type

    def nextValidId(self, orderId:int):
        if self.req_type == 'contracts':
            self.send_contract_details_request(orderId)
        elif self.req_type == 'data':
            self.send_data_request(orderId)

    def send_data_request(self, orderId):
        contract = Contract()
        contract.symbol = self.underlying_code
        contract.secType = "FUT"
        contract.exchange = self.exchange
        contract.currency = "USD"
        contract.lastTradeDateOrContractMonth = self.last_trade_date_or_contract_month
        contract.includeExpired = True

        end_date_time = f'{self.last_trade_date_or_contract_month} 15:00:00 US/Eastern'

        self.reqHistoricalData(orderId, contract, end_date_time, "90 D", "5 mins",
                               "TRADES", 0, 1, False, [])

    def send_contract_details_request(self, orderId):
        print("id", orderId)
        contract = Contract()
        contract.symbol = self.underlying_code
        contract.secType = 'FUT'
        contract.includeExpired = True
        if self.exchange is not None:
            contract.primaryExchange = self.exchange
        if self.last_trade_date_or_contract_month is not None:
            contract.last_trade_date_or_contract_month = self.last_trade_date_or_contract_month

        self.reqContractDetails(10, contract)

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, "", errorCode, "", errorString)

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        self.results.append(contractDetails)

    def contractDetailsEnd(self, reqId:int):
        print("end, disconnecting")
        self.disconnect()

    def historicalData(self, reqId, bar):
        self.results.append(bar)
        #print(f"Historical data: {bar}")

    def historicalDataEnd(self, reqId, start, end):
        print("End of HistoricalData")
        print(f"Start: {start}, End: {end}")
        self.disconnect()

    def get_contracts_dataframe(self):
        my_dict = dict()
        counter = 0
        for item in self.results:
            counter += 1
            my_dict[counter] = {'currency': item.contract.currency,
                                'exchange': item.contract.exchange,
                                'lastTradeDateOrContractMonth': item.contract.lastTradeDateOrContractMonth,
                                'localSymbol': item.contract.localSymbol,
                                'multiplier': item.contract.multiplier,
                                'secType': item.contract.secType,
                                'symbol': item.contract.symbol,
                                'contractMonth': item.contractMonth,
                                'longName': item.longName,
                                'marketName': item.marketName,
                                'realExpirationDate': item.realExpirationDate
                                }
        ct_df = pd.DataFrame.from_dict(my_dict, orient='index')
        ct_df['expi_date'] = pd.to_datetime(ct_df['realExpirationDate'])
        ct_df = ct_df[ct_df['expi_date'] < datetime.now() + timedelta(90)]
        return ct_df.sort_values('expi_date')

    def get_bars_dataframe(self):
        my_dict = dict()
        counter = 0
        for item in self.results:
            counter += 1
            my_dict[counter] = {'datetime': pd.to_datetime(item.date),
                                'open': item.open,
                                'high': item.high,
                                'low': item.low,
                                'close': item.close,
                                'volume': item.volume,
                                'average': item.average,
                                'bar_count': item.barCount,
                                }
        data_df = pd.DataFrame.from_dict(my_dict, orient='index')
        return data_df.set_index('datetime')


def is_already_downloaded(local_symbol):
    for fname in os.listdir('data\\'):
        core_name = fname.split('-')[1].replace('.csv', '')
        if core_name == local_symbol:
            return True
    return False


def main():
    ticker = 'CL'
    exc = 'NYMEX'
    app = TestApp(ticker, exc, 'contracts')
    # app.connect("127.0.0.1", 4002, 0)
    # app.run()
    # contracts = app.get_contracts_dataframe()
    # contracts.to_csv(rf'data\{ticker}-contracts.csv')
    # app.results.clear()
    contracts = pd.read_csv(rf'data\{ticker}-contracts.csv')

    app.set_req_type('data')
    for idx, row in contracts.iterrows():
        if is_already_downloaded(row['localSymbol']):
            continue
        s_time = datetime.now()
        app.last_trade_date_or_contract_month = row['lastTradeDateOrContractMonth']
        app.connect("127.0.0.1", 4002, 0)
        app.run()
        df = app.get_bars_dataframe()
        df.to_csv(rf"data\{ticker}-{row['localSymbol']}.csv")
        app.results.clear()
        print(f'process in {(datetime.now()-s_time).total_seconds()} seconds')


if __name__ == "__main__":
    main()