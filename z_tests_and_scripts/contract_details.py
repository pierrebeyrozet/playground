from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.common import *
from ibapi.contract import *
import pandas as pd
from datetime import datetime, timedelta


class TestApp(EClient, EWrapper):
    def __init__(self, udl_code, exc=None):
        EClient.__init__(self, self)
        self.results = list()
        self.exchange=exc
        self.underlying_code=udl_code

    def nextValidId(self, orderId:int):
        print("id", orderId)
        contract = Contract()
        contract.symbol = self.underlying_code
        contract.secType = 'FUT'
        contract.includeExpired = True
        if self.exchange is not None:
            contract.primaryExchange = self.exchange

        self.reqContractDetails(10, contract)

    def error(self, reqId:TickerId, errorCode:int, errorString:str):
        print("Error: ", reqId, "", errorCode, "", errorString)

    def contractDetails(self, reqId:int, contractDetails:ContractDetails):
        self.results.append(contractDetails)
        # print("contractDetail: ", reqId, " ", contractDetails)

    def contractDetailsEnd(self, reqId:int):
        print("end, disconnecting")
        self.disconnect()

    def get_dataframe(self):
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
        ct_df = ct_df[ct_df['expi_date'] < datetime.now() + timedelta(180)]
        return ct_df.sort_values('expi_date')


def main():
    ticker = 'CL'
    exc = 'NYMEX'
    app = TestApp(ticker, exc)

    app.connect("127.0.0.1", 4002, 0)
    app.run()
    df = app.get_dataframe()
    df.to_csv(rf'data\{ticker}-contracts.csv')

if __name__ == "__main__":
    main()