import httpx
import asyncio

_empty_dict = {}


class AsyncConnector:
    def __init__(self, **config):
        pass

    async def authenticate(self):
        pass

    async def get_conversion(self, **params):
        pass


class InvolveAsyncConnector(AsyncConnector):
    _default_header = {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    _auth_url = "https://api.involve.asia/api/authenticate"
    _all_conversion_url = "https://api.involve.asia/api/conversions/all"
    _range_conversion_url = "https://api.involve.asia/api/conversions/range"

    def __init__(self, **config):
        super().__init__(**config)
        self._secret_key = config.get('secret_key')

        self._token = None

    def token(self):
        return self._token

    async def authenticate(self):
        data = {"secret": self._secret_key, "key": "general"}
        # print(data)
        async with httpx.AsyncClient() as client:
            response = await client.post(self._auth_url, headers=self._default_header, data=data)
            if response.status_code == 200:
                res_body = response.json()
                if res_body.get('status') == "success":
                    self._token = res_body.get('data', _empty_dict).get('token')

    async def get_conversion(self, **params):
        retry = 0 
        while retry < 3:
            try:
                if params.get("start_date") and params.get("end_date"):
                    return await self._get_range_conversion(**params)
                else:
                    return await self._get_all_conversion(**params)
            except Exception as e:
                print(f"Try {retry}")
                retry += 1
                if retry == 3:
                    raise e
                await asyncio.sleep(1)
        return None, False

    async def _fetch_conversion(self, url, data):
        if not self._token:
            raise ValueError("Not authenticated. Please call authenticate() first.")
        headers = {
            **self._default_header,
            "Authorization": f"Bearer {self._token}"
        }
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.post(url, headers=headers, data=data)
            if response.status_code == 200:
                res_body = response.json()
                if res_body.get('status') == "success":
                    data_field = res_body.get('data', {})
                    # nextPage logic
                    has_next_page = data_field.get('nextPage') is not None
                    return (data_field.get('data') or [], has_next_page)
                else:
                    raise Exception(f"API error: {res_body}")
            else:
                raise Exception(f"HTTP error: {response.status_code} - {response.text}")

    async def _get_range_conversion(self, start_date, end_date, page="1", limit="100", **filters):
        data = {"start_date": start_date, "end_date": end_date, "page": page, "limit": limit}
        for key, value in filters.items():
            data[f"filters[{key}]"] = value
        return await self._fetch_conversion(self._range_conversion_url, data)

    async def _get_all_conversion(self, page="1", limit="100", **filters):
        data = {"page": page, "limit": limit}
        for key, value in filters.items():
            data[f"filters[{key}]"] = value
        return await self._fetch_conversion(self._all_conversion_url, data)


# async def test():
#     x = InvolveAsyncConnector(secret_key="H/hyZnEQyqoInz+gXdV6G6fwMCOuGyoxTnTLZYUCxys=")
#     await x.authenticate()
#     # print(x.token())

#     import exporter
#     data = await x.get_conversion(page="1", limit="100")
#     exporter.export_involve_conversion_to_csv("test.csv", data)

# asyncio.run(test())
