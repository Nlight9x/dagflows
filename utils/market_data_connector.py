import httpx
import pandas as pd
from datetime import date, datetime, time
from zoneinfo import ZoneInfo
from utils import tools


supported_market_type = ["base_stock", "derivative"]


class SecuritiesPriceParser:

    _hose_time_sessions = [(time(9, 15), time(11, 30)), (time(13, 0), time(14, 30)), (time(14, 45), time(14, 45))]
    _hnx_time_sessions = [(time(9, 0), time(11, 30)), (time(13, 0), time(14, 30)), (time(14, 45), time(14, 45))]
    _upcom_time_sessions = [(time(9, 0), time(11, 30)), (time(13, 0), time(15, 0))]

    _local_tz = ZoneInfo("Asia/Ho_Chi_Minh")

    _resolution_convert_to_diff_time_map = {
        "1m": "T", "5m": "5T", "30m": "30T", "1h": "H", "1d": "D", "1w": "W"
    }

    _time_session_key_map = {
        "hose_stock": _hose_time_sessions,
        "hnx_derivative": _hnx_time_sessions,
        "hnx_stock": _hnx_time_sessions,
        "upcom_stock": _upcom_time_sessions
    }

    def __init__(self,  **config):
        self._auto_fill_minute_gap = config.get('auto_fill_minute_gap', True)
        self._session_time = config.get('trading_sessions', self._hose_time_sessions)
        self._local_tz = config.get('local_tz', self._local_tz)

    def _get_session_time(self, exchange):
        if not exchange or exchange not in self._time_session_key_map:
            raise ValueError(f"Exchange '{exchange}' not found in time_session_key_map. Available exchanges: {list(self._time_session_key_map.keys())}")
        return self._time_session_key_map[exchange]

    def parse(self, raw_data, symbol=None, **setting) -> pd.DataFrame:
        raise NotImplementedError


class VietstockParser(SecuritiesPriceParser):
    def __init__(self, **config):
        super().__init__(**config)

    def parse_history(self, raw_data, symbol=None, resolution=None, **setting) -> pd.DataFrame:
        if not raw_data:
            columns = ['symbol', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'date']
            return pd.DataFrame(columns=columns)

        if isinstance(raw_data, dict) and 't' in raw_data:
            df = pd.DataFrame({
                'timestamp': raw_data.get('t', []),
                'open': raw_data.get('o', []),
                'high': raw_data.get('h', []),
                'low': raw_data.get('l', []),
                'close': raw_data.get('c', []),
                'volume': raw_data.get('v', []),
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

        if self._auto_fill_minute_gap:
            filled_frames = []
            fred = self._resolution_convert_to_diff_time_map.get(resolution.lower(), 'T')
            is_day_based = fred in ['D', 'W', 'M', 'Y'] or (isinstance(fred, str) and any(fred.startswith(x) for x in ['D', 'W', 'M', 'Y']))
            session_time = self._get_session_time(setting.get('exchange'))
            if not is_day_based:
                for trading_day in sorted(df['date'].unique()):
                    day_df = df[df['date'] == trading_day]
                    session_ranges = []
                    for session_start, session_end in session_time:
                        start_dt = datetime.combine(trading_day, session_start)
                        end_dt = datetime.combine(trading_day, session_end)
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
        
        # Reset index to ensure clean 0-based index (after sort_values, index may not be sequential)
        df = df.reset_index(drop=True)
        df = df[['symbol', 'timestamp', 'datetime', 'open', 'high', 'low', 'close', 'volume', 'date']]
        return df

    def parse_event(self, raw_data, symbol):
        columns = ['symbol', 'timestamp', 'datetime', 'event']
        if not raw_data:
            return pd.DataFrame(columns=columns)
        df = pd.DataFrame(raw_data)
        df['timestamp'] = df['time']
        df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True) \
            .dt.tz_convert(self._local_tz).dt.tz_localize(None)
        df['symbol'] = symbol
        df['event'] = df['text']
        return df[columns]



class SecuritiesMarketConnector:
    def __init__(self,  **setting):
        pass

    def get_history(self, symbol, exchange, **params):
        pass


class VietstockConnector(SecuritiesMarketConnector):
    """Connector for Vietstock API to download stock market data"""
    
    _base_history_url = "https://api.vietstock.vn/tvnew/history"
    _base_event_url = "https://api.vietstock.vn/tvnew/marks"
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

    def _to_base_request_param(self, symbol, **params):
        base_resolution = params.get('resolution', '').lower()
        if base_resolution not in self._resolution_convert_map:
            raise ValueError(f"Resolution '{params['resolution']}' is invalid!")
        rq_params = {
            'symbol': symbol,
            'resolution': self._resolution_convert_map.get(base_resolution),
            'to': int(datetime.now().timestamp()) if 'to_timestamp' not in params else params.get('to_timestamp')}
        rq_params['from'] = rq_params['to'] - 86400 if 'from_timestamp' not in params else params.pop('from_timestamp')

        return rq_params
    
    def get_raw_history(self, symbol, **params):

        if self._client is None:
            raise RuntimeError("Client not initialized. Use VietstockConnector as context manager.")

        rq_params = self._to_base_request_param(symbol, **params)
        response = self._client.get(self._base_history_url, params=rq_params, headers=self._default_headers)
        response.raise_for_status()
        
        return response.json()

    def get_history(self, symbol, exchange, **params):
        raw = self.get_raw_history(symbol, **params)
        return self._parser.parse_history(raw, symbol=symbol, exchange=exchange, resolution=params['resolution'])

    def get_raw_event(self, symbol, **params):
        if self._client is None:
            raise RuntimeError("Client not initialized. Use VietstockConnector as context manager.")

        rq_params = self._to_base_request_param(symbol, **params)

        response = self._client.get(self._base_event_url, params=rq_params, headers=self._default_headers)
        response.raise_for_status()

        return response.json()

    def get_event(self, symbol, **params):
        raw = self.get_raw_event(symbol, **params)
        return self._parser.parse_event(raw_data=raw, symbol=symbol)


# with VietstockConnector(timeout=30.0) as c:
#     today = int(datetime.now().timestamp())
#     from_day = today- 60*60*24*365
#     print(today)
#     print(from_day)
#     df = c.get_event('ACB', from_timestamp=from_day, to_timestamp=today, resolution='1d')
#     with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', None):
#         print(df)
    # print(c.get_history(tools.get_derivative_underlying_codes(date.today()).get('VN30F1M'), exchange='hose_stock', from_timestamp=1726624800, to_timestamp=2114355600, resolution='1d'))