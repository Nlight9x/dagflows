import httpx
import json
from urllib.parse import urlencode


class AdsNetworkAsyncConnector:

    def __init__(self, **config):
        pass

    def authenticate(self):
        pass

    def get_reports(self, **settings):
        pass


class GalaksionAsyncConnector(AdsNetworkAsyncConnector):
    _auth_url = "https://ssp2-api.galaksion.com/api/v1/auth"
    _reports_url = "https://ssp2-api.galaksion.com/api/v1/advertiser/statistics"
    _default_headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    def __init__(self, **config):
        super().__init__()
        self._email = config.get('email')
        self._password = config.get('password')
        
        self._token = None

    async def authenticate(self):
        if not self._email or not self._password:
            raise ValueError("Email and password are required")

        data = {
            "email": self._email,
            "password": self._password
        }

        async with httpx.AsyncClient() as client:
            response = await client.post(
                self._auth_url,
                headers=self._default_headers,
                json=data
            )
            if response.status_code == 200:
                res_body = response.json()
                self._token = res_body.get('token')
                return self._token
            return None

    async def get_reports(self, date_from, date_to, group_by=None, order_by=None, limit=100, offset=0, campaigns=None):
        if not self._token:
            raise ValueError("Not authenticated. Please call authenticate() first")

        # Prepare query parameters
        params = {
            "dateFrom": date_from,
            "dateTo": date_to,
            "limit": limit,
            "offset": offset
        }

        # Add optional parameters if provided
        if group_by:
            params["groupBy"] = json.dumps(group_by)
        if order_by:
            params["orderBy"] = json.dumps(order_by)
        if campaigns:
            params["campaigns"] = json.dumps(campaigns)

        # Prepare headers with auth token
        headers = {
            **self._default_headers,
            "X-Auth-Token": self._token
        }

        # Make the request
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{self._reports_url}?{urlencode(params)}", headers=headers, timeout=30.0)
            if response.status_code == 429:
                # Raise HTTPStatusError để DAG nhận biết và retry
                raise httpx.HTTPStatusError("Too Many Requests", request=response.request, response=response)
            if response.status_code == 200:
                res = response.json()
                # Xác định has_next dựa vào countRows == limit
                if isinstance(res, dict):
                    count_rows = res.get('countRows', 0)
                    has_next = count_rows == limit
                    return res, has_next
                return {}, False
            # Raise cho các lỗi khác
            response.raise_for_status()
            return {}, False

