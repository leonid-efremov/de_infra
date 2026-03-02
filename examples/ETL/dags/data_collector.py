import requests
import json
from typing import Optional, Dict, Any

from utils import DuckdbUtils


SUPPORTED_ASSETS = {
    'BTC': 'BITCOIN',
    'ETH': 'ETHEREUM',
}
ALPHA_VANTAGE_URL = 'https://www.alphavantage.co/query'
API_KEY = ''


class CryptoDataCollector:

    def __init__(self, asset: str, schematable_name: str, schematable_name_stg: str):
        assert asset in SUPPORTED_ASSETS.keys(), f'Supported assets: {SUPPORTED_ASSETS.keys()}'

        self.api_key = API_KEY
        self.base_url = ALPHA_VANTAGE_URL
        self.schematable_name = schematable_name
        self.schematable_name_stg = schematable_name_stg
        self.asset = asset

        self.conn = self._get_conn()

    def _get_conn(self):
        init_conn = DuckdbUtils.setup_db()
        res_conn, self.catalog_name = DuckdbUtils.setup_catalog(init_conn)
        return res_conn

    def fetch_asset_data_json(self, market: str = "USD") -> Optional[Dict[str, Any]]:
        api_params = {
            'function': 'DIGITAL_CURRENCY_DAILY',
            'symbol': self.asset,
            'market': market,
            'apikey': self.api_key
        }

        response = requests.get(self.base_url, params=api_params)
        response.raise_for_status()
        data = response.json()

        return data

    def load_json_to_stg(self, input_data_json: Dict[str, Any]) -> None:
        json_str = json.dumps(input_data_json)

        self.conn.execute(f"""
            INSERT INTO {self.schematable_name_stg} (
                dt, open_price, high_price, low_price, close_price, trading_volume, market_cap, processed_dttm
            )

            WITH parsed_json AS (
                SELECT '{json_str}'::JSON as raw_json
            ),
            time_series_json AS (
                SELECT raw_json->'Time Series (Digital Currency Daily)' as ts_json
                FROM parsed_json
            ),
            exploded AS (
                SELECT key::VARCHAR as date_key,
                    value as daily_data
                FROM (
                    SELECT unnest(
                        list_transform(
                            json_keys(ts_json), k -> {{'key': k, 'value': ts_json -> k}}
                        )
                    ) AS entry
                    FROM time_series_json
                ) AS t(entry)
            )

            SELECT 
                CAST(date_key AS DATE) AS dt,
                CAST(daily_data->>'1a. open (USD)' AS DOUBLE) AS open_price,
                CAST(daily_data->>'2a. high (USD)' AS DOUBLE) AS high_price,
                CAST(daily_data->>'3a. low (USD)' AS DOUBLE) AS low_price,
                CAST(daily_data->>'4a. close (USD)' AS DOUBLE) AS close_price,
                CAST(daily_data->>'5. volume' AS DOUBLE) AS trading_volume,
                CAST(daily_data->>'6. market cap (USD)' AS DOUBLE) AS market_cap,
                CURRENT_TIMESTAMP AS processed_at
            FROM exploded
            ORDER BY dt DESC
            ;
        """)

    def load_from_stg_to_target(self) -> None:
        self.conn.execute(f"""
            INSERT INTO {self.schematable_name}
            SELECT *
            FROM {self.schematable_name_stg};
        """)

    def process(self) -> int:
        res_json = self.fetch_asset_data_json()

        DuckdbUtils.clean_table(self.schematable_name_stg)
        self.load_json_to_stg(res_json)
        row_cnt = DuckdbUtils.check_table_data(self.schematable_name_stg)

        if row_cnt > 0:
            DuckdbUtils.clean_table(self.schematable_name)
            self.load_from_stg_to_target()

        self.conn.close()
        return row_cnt


if __name__ == '__main__':
    sample_asset = 'BTC'

    sample_data_collector = CryptoDataCollector(
        sample_asset,
        'iceberg.cryptocurrencies_project_raw.btc',
        'iceberg.cryptocurrencies_project_raw_stg.btc',
    )
    sample_result_row_cnt = sample_data_collector.process()

    print(f"Successfully loaded {sample_result_row_cnt} rows")
