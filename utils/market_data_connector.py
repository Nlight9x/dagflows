import httpx
import pandas as pd
from datetime import datetime, timedelta, date, time
from zoneinfo import ZoneInfo


class SecuritiesPriceParser:

    _default_time_sessions = [(time(9, 15), time(11, 30)), (time(13, 0), time(14, 30)), (time(14, 45), time(14, 45))]
    _local_tz = ZoneInfo("Asia/Ho_Chi_Minh")

    _resolution_convert_to_diff_time_map = {
        "1m": "T", "5m": "5T", "30m": "30T", "1h": "H", "1d": "D", "1w": "W"
    }

    def __init__(self,  **config):
        self._auto_fill_gap = config.get('auto_fill_gap', True)
        self._session_time = config.get('trading_sessions', self._default_time_sessions)
        self._local_tz = config.get('local_tz', self._local_tz)

    def parse(self, raw_data, symbol=None, **setting) -> pd.DataFrame:
        raise NotImplementedError


class VietstockParser(SecuritiesPriceParser):
    def __init__(self, **config):
        super().__init__(**config)

    def parse(self, raw_data, symbol=None, resolution=None, **setting) -> pd.DataFrame:
        if not raw_data:
            columns = ['symbol', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'date']
            return pd.DataFrame(columns=columns)

        if isinstance(raw_data, dict) and 't' in raw_data:
            timestamps = raw_data.get('t', [])
            opens = raw_data.get('o', [])
            highs = raw_data.get('h', [])
            lows = raw_data.get('l', [])
            closes = raw_data.get('c', [])
            volumes = raw_data.get('v', [])

            df = pd.DataFrame({
                'timestamp': timestamps,
                'open': opens,
                'high': highs,
                'low': lows,
                'close': closes,
                'volume': volumes,
            })
        elif isinstance(raw_data, list):
            df = pd.DataFrame(raw_data)
        else:
            df = pd.DataFrame()

        if df.empty:
            columns = ['symbol', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'date']
            return pd.DataFrame(columns=columns)

        if 'timestamp' not in df.columns:
            if 't' in df.columns:
                df.rename(columns={'t': 'timestamp'}, inplace=True)
            else:
                raise ValueError("Raw Vietstock data must include 'timestamp' column")

        df['timestamp'] = df['timestamp'].astype(int)
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)\
            .dt.tz_convert(self._local_tz).dt.tz_localize(None)
        df.sort_values('datetime', inplace=True)

        if 'symbol' not in df.columns:
            df['symbol'] = symbol
        elif symbol is not None:
            df['symbol'] = symbol

        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = None

        df['date'] = df['datetime'].dt.date

        if self._auto_fill_gap:
            filled_frames = []
            for trading_day in sorted(df['date'].unique()):
                day_df = df[df['date'] == trading_day]
                session_ranges = []
                for session_start, session_end in self._session_time:
                    start_dt = datetime.combine(trading_day, session_start)
                    end_dt = datetime.combine(trading_day, session_end)
                    fred = self._resolution_convert_to_diff_time_map.get(resolution, 'T')
                    session_ranges.append(pd.date_range(start_dt, end_dt, freq=fred))

                if session_ranges:
                    expected_index = pd.DatetimeIndex(sorted(set().union(*session_ranges)))
                    day_df = day_df.set_index('datetime').reindex(expected_index)
                    day_df['symbol'] = day_df['symbol'].fillna(symbol)
                    day_df['timestamp'] = (day_df.index.view('int64') // 10**9).astype(int)
                    day_df['volume'] = day_df['volume'].fillna(0)
                    day_df['close'] = day_df['close'].ffill()
                    day_df['open'] = day_df['open'].fillna(day_df['close'])
                    day_df['high'] = day_df['high'].fillna(day_df['close'])
                    day_df['low'] = day_df['low'].fillna(day_df['close'])
                    day_df['date'] = trading_day
                    day_df = day_df.reset_index().rename(columns={'index': 'datetime'})
                else:
                    day_df = day_df.reset_index(drop=True)

                filled_frames.append(day_df)

            df = pd.concat(filled_frames, ignore_index=True)
        else:
            df = df.reset_index(drop=True)

        df = df[['symbol', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'date']]
        return df


class SecuritiesMarketConnector:
    def __init__(self,  **setting):
        pass

    def get_history(self, symbol, **params):
        pass


class VietstockConnector(SecuritiesMarketConnector):
    """Connector for Vietstock API to download stock market data"""
    
    _base_url = "https://api.vietstock.vn/tvnew/history"
    _default_headers = {
        'Accept': '*/*',
        'Origin': 'https://stockchart.vietstock.vn',
        'Referer': 'https://stockchart.vietstock.vn/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    }

    _resolution_convert_map = {
        "1m": "1", "5m": "5", "30m": "30", "1h": "60", "1d": "1D", "1w": "1W"
    }
    
    def __init__(self, **setting):
        """
        Initialize Vietstock connector
        
        Args:
            timeout: Request timeout in seconds (default: 30.0)
        """
        super().__init__(**setting)
        self._timeout = setting.get('timeout', 30)
        self._parser = setting.get('parser', VietstockParser())

        self._client = None
    
    def __enter__(self):
        """Context manager entry"""
        self._client = httpx.Client(timeout=self._timeout)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self._client:
            self._client.close()
    
    def get_raw_history(self, symbol, **params):

        if self._client is None:
            raise RuntimeError("Client not initialized. Use VietstockConnector as context manager.")

        base_resolution = params['resolution'].lower()
        if base_resolution not in self._resolution_convert_map:
            raise ValueError(f"Resolution '{params['resolution']}' is invalid!")

        params['symbol'] = symbol
        params['resolution'] = self._resolution_convert_map.get(base_resolution)
        params['to'] = int(datetime.now().timestamp()) if 'to_timestamp' not in params else params.pop('to_timestamp')
        params['from'] = params['to'] - 86400 if 'from_timestamp' not in params else params.pop('from_timestamp')

        print(params)
        
        response = self._client.get(self._base_url, params=params, headers=self._default_headers)
        response.raise_for_status()
        
        return response.json()

    def get_history(self, symbol, **params):
        raw = self.get_raw_history(symbol, **params)
        return self._parser.parse(raw, symbol=symbol, resolution=params['resolution'])

