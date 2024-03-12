from ib_insync import *

ib = IB()
ib.connect('127.0.0.1', 4002, clientId=1)

#contract = Forex('EURUSD')
contract = Future('CL', '202403', exchange='NYMEX')
bars = ib.reqHistoricalData(
    contract, endDateTime='', durationStr='30 D',
    barSizeSetting='1 hour', whatToShow='MIDPOINT', useRTH=True)

# convert to pandas dataframe (pandas needs to be installed):
df = util.df(bars)
print(df)