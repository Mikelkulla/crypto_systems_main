import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from math import sqrt, log
import googlesheets_get_functions as gsh_get
import config as conf
from logging_config import setup_logger

logger = setup_logger(__name__)

def fdi_adaptive_supertrend(
    df,
    per=30,
    speed=20,
    mult=3.0,
    adapt=True,
    fl_lookback=25,
    fl_level_up=80.0,
    fl_level_down=20.0
):
    """
    Calculate FDI-Adaptive Supertrend with Floating Levels.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with columns ['Date', 'Open', 'High', 'Low', 'Close', 'Volume (USDT)'].
    per : int
        Fractal period ingest (default: 30).
    speed : int
        Speed parameter (default: 20).
    mult : float
        Multiplier for ATR bands (default: 3.0).
    adapt : bool
        Whether to use adaptive period (default: True).
    fl_lookback : int
        Floating level lookback period (default: 25).
    fl_level_up : float
        Floating levels up percentage (default: 80.0).
    fl_level_down : float
        Floating levels down percentage (default: 20.0).
    
    Returns:
    --------
    dict
        Contains:
        - date: Array of dates
        - supertrend: Supertrend values
        - direction: Trend direction (1 for up, -1 for down)
        - flu: Upper floating level
        - fld: Lower floating level
        - flm: Middle floating level
        - go_long: Boolean array for long signals
        - go_short: Boolean array for short signals
    """
    try:
        logger.info("Starting FDI-Adaptive Supertrend calculation")

        # Extract data
        # === Price Source Options ===
        # Uncomment the desired source calculation

        # src = (df['High'] + df['Low']) / 2  # hl2 - (High + Low) / 2
        src = (df['Open'] + df['High'] + df['Low'] + df['Close']) / 4  # ohlc4 - (Open + High + Low + Close) / 4
        # src = df['Close']  # Close
        # src = df['Open']  # Open
        # src = df['High']  # High
        # src = df['Low']  # Low
        # src = (df['High'] + df['Low']) / 2  # default (hl2)

        src = np.asarray(src)
        high = np.asarray(df['High'])
        low = np.asarray(df['Low'])
        close = np.asarray(df['Close'])
        dates = np.asarray(df['Date'])
        n = len(src)

        logger.debug(f"Processing {n} data points for FDI-Adaptive Supertrend")

        # Rolling min/max
        def rolling_min(series, window):
            return pd.Series(series).rolling(window=window, min_periods=1).min().values
        
        def rolling_max(series, window):
            return pd.Series(series).rolling(window=window, min_periods=1).max().values
        
        # FDI calculation
        def fdip(src, per, speedin):
            fmax = rolling_max(src, per)
            fmin = rolling_min(src, per)
            length = np.zeros_like(src)
            
            for i in range(per - 1, n):
                diff = np.zeros(per)
                for j in range(per):
                    if i - j >= 0:
                        diff[j] = (src[i - j] - fmin[i]) / (fmax[i] - fmin[i]) if fmax[i] != fmin[i] else 0
                for j in range(per - 1):
                    if j < len(diff) - 1:
                        term = sqrt((diff[j] - diff[j + 1])**2 + (1 / per**2))
                        length[i] += term
                length[i] = 1 + (log(length[i]) + log(2)) / log(2 * per) if length[i] > 0 else 1
                traildim = 1 / (2 - length[i])
                alpha = traildim / 2
                length[i] = round(speedin * alpha)
            
            return np.maximum(length, 1)
        
        # RMA for ATR
        def rma(series, period):
            ema = np.zeros_like(series)
            for i in range(n):
                if i == 0:
                    ema[i] = series[i]
                else:
                    ema[i] = (series[i] - ema[i-1]) * (1/period) + ema[i-1]
            return ema
        
        # True Range
        logger.debug("Calculating True Range")

        tr = np.zeros_like(src)
        for i in range(1, n):
            tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
        
        # Adaptive period
        logger.debug("Calculating FDI for adaptive period")

        masterdom = fdip(src, per, speed)
        len_ = np.floor(masterdom).astype(int)
        len_ = np.maximum(len_, 1)
        
        # ATR
        logger.debug("Calculating ATR")
        atr_period = len_ if adapt else np.full(n, per)
        atr = np.zeros_like(src)
        for i in range(n):
            atr[i] = rma(tr, atr_period[i])[i]
        
        # Supertrend
        logger.debug("Calculating Supertrend bands")
        upper_band = src + mult * atr
        lower_band = src - mult * atr
        direction = np.zeros(n, dtype=int)
        supertrend = np.zeros(n)
        
        for i in range(1, n):
            prev_lower_band = lower_band[i-1]
            prev_upper_band = upper_band[i-1]
            
            lower_band[i] = lower_band[i] if lower_band[i] > prev_lower_band or close[i-1] < prev_lower_band else prev_lower_band
            upper_band[i] = upper_band[i] if upper_band[i] < prev_upper_band or close[i-1] > prev_upper_band else prev_upper_band
            
            if i == 1 and np.isnan(atr[i-1]):
                direction[i] = -1  # Initial direction: downtrend
            else:
                prev_supertrend = supertrend[i-1]
                if prev_supertrend == prev_upper_band:
                    direction[i] = 1 if close[i] > upper_band[i] else -1  # Uptrend if price crosses above upper band
                else:
                    direction[i] = -1 if close[i] < lower_band[i] else 1  # Downtrend if price crosses below lower band
            
            supertrend[i] = lower_band[i] if direction[i] == 1 else upper_band[i]
        
        # Floating levels
        logger.debug("Calculating floating levels")
        mini = rolling_min(supertrend, fl_lookback)
        maxi = rolling_max(supertrend, fl_lookback)
        rrange = maxi - mini
        flu = mini + fl_level_up * rrange / 100.0
        fld = mini + fl_level_down * rrange / 100.0
        flm = mini + 0.5 * rrange
        
        # Signals
        logger.debug("Generating long/short signals")
        go_long = np.zeros(n, dtype=bool)
        go_short = np.zeros(n, dtype=bool)
        for i in range(1, n):
            go_long[i] = direction[i] == 1 and direction[i-1] == -1
            go_short[i] = direction[i] == -1 and direction[i-1] == 1
        
        logger.info("FDI-Adaptive Supertrend calculation completed")
        return {
            'date': dates,
            'supertrend': supertrend,
            'direction': direction,
            'flu': flu,
            'fld': fld,
            'flm': flm,
            'go_long': go_long,
            'go_short': go_short
        }
    except Exception as e:
        logger.error(f"Error in FDI-Adaptive Supertrend calculation: {str(e)}")
        raise

def plot_fdi_adaptive_supertrend(df, result):
    """
    Plot FDI-Adaptive Supertrend with Floating Levels and signals.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with price data.
    result : dict
        Output from fdi_adaptive_supertrend.
    """
    plt.figure(figsize=(14, 8))
    
    # Close price
    plt.plot(df['Date'], df['Close'], label='Close Price', color='blue', alpha=0.5)
    
    # Supertrend as a single continuous line with dynamic colors
    supertrend = result['supertrend']
    direction = result['direction']
    dates = df['Date']
    
    for i in range(1, len(dates)):
        color = 'green' if direction[i] == 1 else 'red'
        plt.plot(dates[i-1:i+1], supertrend[i-1:i+1], color=color, linewidth=2)
    
    plt.plot([], [], color='green', label='Supertrend (Up)', linewidth=2)
    plt.plot([], [], color='red', label='Supertrend (Down)', linewidth=2)
    
    # Floating levels
    plt.plot(df['Date'], result['flu'], '--', color='green', label='Upper Float (80%)', alpha=0.5)
    plt.plot(df['Date'], result['fld'], '--', color='red', label='Lower Float (20%)', alpha=0.5)
    plt.plot(df['Date'], result['flm'], '--', color='gray', label='Middle Float (50%)', alpha=0.5)
    
    # Signals
    go_long = result['go_long']
    go_short = result['go_short']
    plt.scatter(df['Date'][go_long], df['Close'][go_long], marker='^', color='yellow', label='Long Signal', s=100)
    plt.scatter(df['Date'][go_short], df['Close'][go_short], marker='v', color='fuchsia', label='Short Signal', s=100)
    
    # Customize x-axis to show ticks every 3 days
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Ticks every 3 days
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))  # Format as YYYY-MM-DD
    
    # Alternative option:
    # - Ticks every 5 days:
    # ax.xaxis.set_major_locator(mdates.DayLocator(interval=5))
    # Customize
    plt.title('FDI-Adaptive Supertrend with Floating Levels')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

def liquidity_weighted_supertrend(
    df,
    factor=2.5,
    period=75,
    fast_ma_length=46,
    slow_ma_length=65,
    supertrend_type="Smoothed"
):
    """
    Calculate Liquidity Weighted Supertrend indicator.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with columns ['Date', 'Open', 'High', 'Low', 'Close', 'Volume (USDT)'].
    factor : float
        Multiplier for ATR (default: 1.0).
    period : int
        Supertrend ATR period (default: 20).
    fast_ma_length : int
        Fast moving average length (default: 10).
    slow_ma_length : int
        Slow moving average length (default: 50).
    supertrend_type : str
        Type of Supertrend ("Aggressive" or "Smoothed", default: "Smoothed").
    
    Returns:
    --------
    dict
        Contains:
        - date: Array of dates
        - supertrend: Supertrend values
        - direction: Trend direction (1 for up, -1 for down)
        - go_long: Boolean array for long signals
        - go_short: Boolean array for short signals
    """
    try:
        logger.info("Starting Liquidity Weighted Supertrend calculation")
      
        # Extract data
        close = np.asarray(df['Close'])
        volume = np.asarray(df['Volume (USDT)'])
        dates = np.asarray(df['Date'])
        high = np.asarray(df['High'])
        low = np.asarray(df['Low'])
        n = len(close)

        logger.debug(f"Processing {n} data points for Liquidity Weighted Supertrend")
        
        # Liquidity calculation: volume * close
        liquidity = volume * close
        
        # Weighted sums for fast and slow MAs
        def weighted_sum(data, liquidity, length):
            result = np.zeros(n)
            for i in range(n):
                start = max(0, i - length + 1)
                result[i] = np.sum(data[start:i+1] * liquidity[start:i+1])
            return result
        
        def liquidity_sum(liquidity, length):
            result = np.zeros(n)
            for i in range(n):
                start = max(0, i - length + 1)
                result[i] = np.sum(liquidity[start:i+1])
            return result

        logger.debug("Calculating weighted moving averages")
        weighted_sum_fast = weighted_sum(close, liquidity, fast_ma_length)
        weighted_sum_slow = weighted_sum(close, liquidity, slow_ma_length)
        liquidity_sum_fast = liquidity_sum(liquidity, fast_ma_length)
        liquidity_sum_slow = liquidity_sum(liquidity, slow_ma_length)
        
        # Liquidity-weighted moving averages
        liquidity_weighted_ma_fast = np.where(liquidity_sum_fast != 0, weighted_sum_fast / liquidity_sum_fast, close)
        liquidity_weighted_ma_slow = np.where(liquidity_sum_slow != 0, weighted_sum_slow / liquidity_sum_slow, close)
        
        # Select MA based on supertrend_type
        hl2_lwma = liquidity_weighted_ma_fast if supertrend_type == "Aggressive" else liquidity_weighted_ma_slow
        
        # ATR calculation
        def atr(high, low, close, period):
            tr = np.zeros(n)
            for i in range(1, n):
                tr[i] = max(high[i] - low[i], abs(high[i] - close[i-1]), abs(low[i] - close[i-1]))
            atr = pd.Series(tr).rolling(window=period, min_periods=1).mean().values
            return atr
        
        logger.debug("Calculating ATR")
        atr_val = atr(high, low, close, period)
        
        # Supertrend calculation
        logger.debug("Calculating Supertrend bands")
        up_band = hl2_lwma - (factor * atr_val)
        down_band = hl2_lwma + (factor * atr_val)
        
        trend_up = np.copy(up_band)
        trend_down = np.copy(down_band)
        direction = np.ones(n, dtype=int)  # 1 for uptrend, -1 for downtrend
        supertrend = np.zeros(n)
        
        for i in range(1, n):
            # Update TrendUp
            if close[i-1] > trend_up[i-1]:
                trend_up[i] = max(up_band[i], trend_up[i-1])
            else:
                trend_up[i] = up_band[i]
            
            # Update TrendDown
            if close[i-1] < trend_down[i-1]:
                trend_down[i] = min(down_band[i], trend_down[i-1])
            else:
                trend_down[i] = down_band[i]
            
            # Determine trend direction
            if close[i] <= trend_down[i-1]:
                if close[i] < trend_up[i-1]:
                    direction[i] = -1
                else:
                    direction[i] = direction[i-1]
            else:
                direction[i] = 1
            
            # Set Supertrend value
            supertrend[i] = trend_up[i] if direction[i] == 1 else trend_down[i]
        
        # Initialize first supertrend value
        supertrend[0] = trend_up[0] if direction[0] == 1 else trend_down[0]
        
        # Signals
        logger.debug("Generating long/short signals")
        go_long = np.zeros(n, dtype=bool)
        go_short = np.zeros(n, dtype=bool)
        for i in range(1, n):
            # Cross above supertrend (go long)
            if close[i] > supertrend[i] and close[i-1] <= supertrend[i-1]:
                go_long[i] = True
            # Cross below supertrend (go short)
            if close[i] < supertrend[i] and close[i-1] >= supertrend[i-1]:
                go_short[i] = True
        
        logger.info("Liquidity Weighted Supertrend calculation completed")
        return {
            'date': dates,
            'supertrend': supertrend,
            'direction': direction,
            'go_long': go_long,
            'go_short': go_short
        }
    except Exception as e:
        logger.error(f"Error in Liquidity Weighted Supertrend calculation: {str(e)}")
        raise

def plot_liquidity_weighted_supertrend(df, result):
    """
    Plot Liquidity Weighted Supertrend with signals.
    
    Parameters:
    -----------
    df : pd.DataFrame
        DataFrame with price data.
    result : dict
        Output from liquidity_weighted_supertrend.
    """
    plt.figure(figsize=(14, 8))
    
    # Close price
    plt.plot(df['Date'], df['Close'], label='Close Price', color='blue', alpha=0.5)
    
    # Supertrend as a single continuous line with dynamic colors
    supertrend = result['supertrend']
    direction = result['direction']
    dates = df['Date']
    
    for i in range(1, len(dates)):
        color = 'green' if direction[i] == 1 else 'red'
        plt.plot(dates[i-1:i+1], supertrend[i-1:i+1], color=color, linewidth=2)
    
    plt.plot([], [], color='green', label='Supertrend (Up)', linewidth=2)
    plt.plot([], [], color='red', label='Supertrend (Down)', linewidth=2)
    
    # Signals
    go_long = result['go_long']
    go_short = result['go_short']
    plt.scatter(df['Date'][go_long], df['Close'][go_long], marker='^', color='yellow', label='Long Signal', s=100)
    plt.scatter(df['Date'][go_short], df['Close'][go_short], marker='v', color='fuchsia', label='Short Signal', s=100)
    
    # Customize x-axis to show ticks every 3 days
    ax = plt.gca()
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    
    # Customize
    plt.title('Liquidity Weighted Supertrend')
    plt.xlabel('Date')
    plt.ylabel('Price')
    plt.legend()
    plt.grid(True)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.show()

# Test with Google Sheets data
if __name__ == "__main__":
    credentials_file = conf.GOOGLE_PROJECT_CREDENTIALS
    history_prices_daily_spreadsheet_name = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_NAME
    history_prices_daily_spreadsheet_url = conf.SPREADSHEET_HISTORICAL_PRICES_DAILY_URL

    # Systems Spreadsheet
    systems_spreadsheet_name = 'RSPS LV3'
    systems_coins_list_sheet_name = 'Coins'
    systems_beta_targetsheet_name = '5.1 - Beta'
    systems_beta_targetrange_name_BTC = 'B5:C31'
    systems_beta_targetrange_name_TOTAL = 'B4:D31'
    beta_days = 600

    df = gsh_get.get_coin_historical_prices_from_google_sheets(history_prices_daily_spreadsheet_name, credentials_file, 'RENDER')
    logger.info("Fetched historical prices for RENDER")
    print('==================================')
    print(df['Date'])
    # Ensure dates are in datetime format and sorted
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.sort_values('Date').reset_index(drop=True)
    
    # Run indicator
    logger.info("Running FDI-Adaptive Supertrend for test")
    result = fdi_adaptive_supertrend(df)
    
    # Find the index where the direction switches to -1 around March 2025
    target_date = pd.to_datetime('2025-03-09')
    target_date_end = pd.to_datetime('2025-03-11')
    mask = (result['date'] >= target_date) & (result['date'] <= target_date_end)
    indices = np.where(mask)[0]
    
    # Print data around the switch
    logger.debug("Printing data around March 9-10, 2025")
    print("\nData around March 9-10, 2025:")
    for idx in indices:
        print(f"Date: {result['date'][idx]}, Close: {df['Close'].iloc[idx]}, "
              f"High: {df['High'].iloc[idx]}, Low: {df['Low'].iloc[idx]}, "
              f"Supertrend: {result['supertrend'][idx]:.2f}, Direction: {result['direction'][idx]}")
    
    # Print direction changes
    logger.debug("Printing direction changes around March 2025")
    print("\nDirection Changes around March 2025:")
    for i in range(max(0, indices[0] - 5), min(len(result['date']), indices[-1] + 5)):
        if i > 0 and result['direction'][i] != result['direction'][i-1]:
            print(f"Direction change at {result['date'][i]}: {result['direction'][i-1]} -> {result['direction'][i]}")
    
    # Print last 110 days
    logger.debug("Printing last 110 days of data")
    print_days = -110
    print("\nLast 110 Days:")
    print("Dates:", result['date'][print_days:])
    print("Supertrend:", result['supertrend'][print_days:])
    print("Direction:", result['direction'][print_days:])
    print("Upper Float:", result['flu'][print_days:])
    print("Lower Float:", result['fld'][print_days:])
    print("Middle Float:", result['flm'][print_days:])
    print("Long Signals:", result['go_long'][print_days:])
    print("Short Signals:", result['go_short'][print_days:])
    
    # Plot
    logger.info("Generating plot for FDI-Adaptive Supertrend")
    plot_fdi_adaptive_supertrend(df, result)