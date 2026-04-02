import httpx
import asyncio

_empty_dict = {}


class AffAsyncConnector:
    def __init__(self, **config):
        pass

    async def authenticate(self):
        pass

    async def get_conversion(self, **params):
        pass


class InvolveAsyncConnector(AffAsyncConnector):
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
                retry += 1
                if retry < 3:
                    print(f"Retrying connection to InvolveAsia, attempt {retry+1}/3...")
                    await asyncio.sleep(20)
                else:
                    print(f"InvolveAsia connection failed after 3 attempts: {e}")
                    raise
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
            print('data')
        return await self._fetch_conversion(self._range_conversion_url, data)

    async def _get_all_conversion(self, page="1", limit="100", **filters):
        data = {"page": page, "limit": limit}
        for key, value in filters.items():
            data[f"filters[{key}]"] = value
            print('data')
        return await self._fetch_conversion(self._all_conversion_url, data)


class TripComAsyncConnector(AffAsyncConnector):

    _get_conversion_url = 'https://www.trip.com/restapi/soa2/18073/json/getAllianceOrderDetail'

    def __init__(self, **config):
        super().__init__(**config)
        self._cookies = config.get('cookies')

        self._df_aid = config.get('df_aid', 5534971)
        self._df_data_type = config.get('df_data_type', 'B')

    async def authenticate(self):
        pass

    async def get_conversion(self, **params):
        retry = 0
        while retry < 3:
            try:
                if not self._cookies:
                    raise ValueError("Not authenticated. Please call authenticate() first.")
                headers = {
                    "Cookie": self._cookies
                }
                async with httpx.AsyncClient(timeout=30.0) as client:
                    page = int(params.get('page', '1'))
                    page_size = int(params.get('page_size', '10'))
                    rq_body = {
                        "aid": params.get('aid', self._df_aid), "dataType": params.get('data_type', self._df_data_type),
                        "pageIndex": page, "pageSize": page_size,
                        "beginDate": params.get("start_date"), "endDate": params.get("end_date"), **params
                    }
                    response = await client.post(self._get_conversion_url, headers=headers, data=rq_body)
                    if response.status_code == 200:
                        res_body = response.json()
                        if res_body.get('ResponseStatus').get('Ack') == "Success":
                            order_count = res_body.get('orderCount', 0)
                            has_next_page = (page * page_size) < order_count
                            return res_body.get('allianceOrderList') or [], has_next_page
                        else:
                            raise Exception(f"API error: {res_body}")
                    else:
                        raise Exception(f"HTTP error: {response.status_code} - {response.text}")
            except Exception as e:
                retry += 1
                if retry < 3:
                    print(f"Retrying connection to Trip.com, attempt {retry + 1}/3...")
                    await asyncio.sleep(20)
                else:
                    print(f"Trip.com connection failed after 3 attempts: {e}")
                    raise
        return None, False


class EcomobiAsyncConnector(AffAsyncConnector):
    _get_conversion_url ='https://api.ecotrackings.com/api/v3/conversions'

    def __init__(self, **config):
        super().__init__(**config)
        self._token_private = config.get("token_private") or config.get("token")

    async def get_conversion(self, **params):
        retry = 0
        while retry < 3:
            try:
                if not self._token_private:
                    raise ValueError("Missing token_private. Please pass token_private or set it in connector config.")
                rq_body = {
                    "token_private": self._token_private, "start_date": params.get("start_date"), "end_date": params.get("end_date"),
                    "status": params.get("status"), "limit": params.get("limit", "100"), "page": params.get("page", "1"),
                    "currency": params.get("currency", "VND"),
                }
                q = {k: v for k, v in rq_body.items() if v not in (None, "", [])}

                async with httpx.AsyncClient(timeout=30.0) as client:
                    response = await client.get(self._get_conversion_url, params=q)
                    response.raise_for_status()
                    res_body = response.json()

                # Best-effort extraction: API may return either list or {data: [...]}
                data = None
                if res_body or "data" not in res_body:
                    if isinstance(res_body.get("data"), list):
                        data = res_body.get("data")
                        payout_field = ["payout_pending", "payout_expect", "payout_approved", "payout_rejected"]
                        for r in data:
                            r['id'] = int(r['adv_order_id'])
                            r['sale_amount'] = f"{r['sale_amount']:,.0f} VND"
                            for f in payout_field:
                                if r[f] not in [None, 0, 0.0]:
                                    r['payout'] = f"{r[f]:,.0f} VND"
                                    break
                else:
                    raise Exception(f"Unexpected API response format: {res_body}")

                has_next_page = False
                if isinstance(res_body, dict):
                    meta = res_body.get("meta") or {}
                    if isinstance(meta, dict):
                        total = meta.get("total")
                        skip = meta.get("skip")
                        meta_limit = meta.get("limit")
                        if total is not None and skip is not None:
                            try:
                                has_next_page = int(skip) + len(data) < int(total)
                            except Exception:
                                has_next_page = False
                        elif total is not None and meta_limit is not None:
                            try:
                                page = int(q.get("page", 1))
                                limit = int(meta_limit)
                                has_next_page = (page * limit) < int(total)
                            except Exception:
                                has_next_page = False
                return data, has_next_page
            except Exception as e:
                retry += 1
                if retry < 3:
                    print(f"Retrying connection to Ecomobi, attempt {retry + 1}/3...")
                    await asyncio.sleep(10)
                else:
                    print(f"Ecomobi connection failed after 3 attempts: {e}")
                    raise
        return None, False


async def _test_ecomobi():
    connector = EcomobiAsyncConnector(token="MkxzcGooVxvEmhRUETkms")
    all_rows = []
    page = 1
    limit = 50

    while True:
        rows, has_next = await connector.get_conversion(
            start_date="2026-03-21",
            end_date="2026-03-31",
            # status="approved",
            limit=str(limit),
            page=str(page),
        )
        print(f"page={page} rows={len(rows)} has_next={has_next}")
        all_rows.extend(rows)
        # break
        if not has_next:
            break
        page += 1

    print(f"TOTAL rows: {len(all_rows)}")
    if all_rows:
        # print("FIRST ROW:", all_rows[0])
        order_ids = set()
        for r in all_rows:
            order_ids.add(r.get('adv_order_id'))
            # print(r)
            if r.get('sub1') is not None:
                print(r)
        print(f"TOTAL set order_id: {len(order_ids)}")

if __name__ == "__main__":
    asyncio.run(_test_ecomobi())
