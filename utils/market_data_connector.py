import httpx


class VietstockConnector:
    """Connector for Vietstock API to download stock market data"""
    
    _base_url = "https://api.vietstock.vn/tvnew/history"
    _default_headers = {
        'Accept': '*/*',
        'Origin': 'https://stockchart.vietstock.vn',
        'Referer': 'https://stockchart.vietstock.vn/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    }
    
    def __init__(self, timeout=30.0):
        """
        Initialize Vietstock connector
        
        Args:
            timeout: Request timeout in seconds (default: 30.0)
        """
        self.timeout = timeout
        self.client = None
    
    def __enter__(self):
        """Context manager entry"""
        self.client = httpx.Client(timeout=self.timeout)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit"""
        if self.client:
            self.client.close()
    
    def get_history(self, symbol, resolution='1', from_timestamp=None, to_timestamp=None):
        """
        Get historical stock data from Vietstock API
        
        Args:
            symbol: Stock symbol (e.g., 'HPG', 'VNM')
            resolution: Time resolution (default: '1')
            from_timestamp: Start timestamp (UNIX timestamp as string)
            to_timestamp: End timestamp (UNIX timestamp as string)
            
        Returns:
            dict: API response data
            
        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        params = {
            'symbol': symbol,
            'resolution': resolution
        }
        
        if from_timestamp:
            params['from'] = from_timestamp
        
        if to_timestamp:
            params['to'] = to_timestamp
        
        if self.client is None:
            raise RuntimeError("Client not initialized. Use VietstockConnector as context manager.")
        
        response = self.client.get(
            self._base_url,
            params=params,
            headers=self._default_headers
        )
        response.raise_for_status()
        
        return response.json()


class VietstockAsyncConnector:
    """Async connector for Vietstock API to download stock market data"""
    
    _base_url = "https://api.vietstock.vn/tvnew/history"
    _default_headers = {
        'Accept': '*/*',
        'Origin': 'https://stockchart.vietstock.vn',
        'Referer': 'https://stockchart.vietstock.vn/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36'
    }
    
    def __init__(self, timeout=30.0):
        """
        Initialize Vietstock async connector
        
        Args:
            timeout: Request timeout in seconds (default: 30.0)
        """
        self.timeout = timeout
        self.client = None
    
    async def __aenter__(self):
        """Async context manager entry"""
        self.client = httpx.AsyncClient(timeout=self.timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.client:
            await self.client.aclose()
    
    async def get_history(self, symbol, resolution='1', from_timestamp=None, to_timestamp=None):
        """
        Get historical stock data from Vietstock API (async)
        
        Args:
            symbol: Stock symbol (e.g., 'HPG', 'VNM')
            resolution: Time resolution (default: '1')
            from_timestamp: Start timestamp (UNIX timestamp as string)
            to_timestamp: End timestamp (UNIX timestamp as string)
            
        Returns:
            dict: API response data
            
        Raises:
            httpx.HTTPStatusError: If the request fails
        """
        params = {
            'symbol': symbol,
            'resolution': resolution
        }
        
        if from_timestamp:
            params['from'] = from_timestamp
        
        if to_timestamp:
            params['to'] = to_timestamp
        
        if self.client is None:
            raise RuntimeError("Client not initialized. Use VietstockAsyncConnector as async context manager.")
        
        response = await self.client.get(
            self._base_url,
            params=params,
            headers=self._default_headers
        )
        response.raise_for_status()
        
        return response.json()

