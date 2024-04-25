
def breakout_up(df):
    if df['close'].iloc[-1] > df['high'].iloc[:df.shape[0]-1].max():
        return True
    return False