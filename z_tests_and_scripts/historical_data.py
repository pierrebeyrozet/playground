from ibapi.client import EClient
from ibapi.wrapper import EWrapper
from ibapi.contract import Contract
import datetime


class TestApp(EClient, EWrapper):
    def __init__(self):
        EClient.__init__(self, self)
        self.results = []

    def nextValidId(self, orderId: int):
        # Get the current year and month
        now = datetime.datetime.now()
        year = now.year
        month = now.month

        contract = Contract()
        contract.symbol = "CL"
        contract.secType = "FUT"
        contract.exchange = "NYMEX"
        contract.currency = "USD"
        # contract.symbol = 'INFY'
        # contract.secType = "FUT"
        # contract.currency = "INR"
        # contract.exchange = "NSE"
        #contract.localSymbol = "ESZ7"  # Set the local symbol
        contract.lastTradeDateOrContractMonth = '20220920'
        contract.includeExpired = True
        self.reqHistoricalData(orderId, contract, "20220920 14:00:00 US/Eastern", "10 D", "1 hour",
                               "TRADES", 0, 1, False, [])

    def historicalData(self, reqId, bar):
        self.results.append(bar)
        print(f"Historical data: {bar}")

    def historicalDataEnd(self, reqId, start, end):
        print("End of HistoricalData")
        print(f"Start: {start}, End: {end}")
        self.disconnect()


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

app = TestApp()
app.connect('127.0.0.1', 4002, 1)
app.run()
a = 0