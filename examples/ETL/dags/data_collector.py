import os
import requests
import json
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import pandas as pd

from utils import DuckdbUtils


SUPPORTED_ASSETS = {
    'BTC': 'BITCOIN',
    'ETH': 'ETHEREUM',
}
ALPHA_VANTAGE_URL = 'https://www.alphavantage.co/query'
load_dotenv() # dotenv_path='../.env'
API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY") # must be specified in your /examples/ETL/.env file


class CryptoDataCollector:

    def __init__(self, asset: str, schematable_name: str, schematable_name_stg: str):
        assert asset in SUPPORTED_ASSETS.keys(), f'Supported assets: {SUPPORTED_ASSETS.keys()}'

        self.api_key = API_KEY
        self.base_url = ALPHA_VANTAGE_URL
        self.schematable_path = schematable_name
        self.schematable_path_stg = schematable_name_stg
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
            'apikey': self.api_key,
        }

        response = requests.get(self.base_url, params=api_params)
        data = response.json()

        return data

    def load_json_to_stg(self, input_data_json: Dict[str, Any]) -> None:
        json_data_str = json.dumps(input_data_json['Time Series (Digital Currency Daily)'])

        pdf = pd.read_json(json_data_str, orient='index')
        pdf = pdf.reset_index().rename(columns={'index': 'date'})

        tmp_table_name = 'tmp_data'
        self.conn.register(tmp_table_name, pdf)
        self.conn.execute(f"""
            COPY (
                SELECT CAST("date" AS DATE) AS dt,
                    CAST("1. open" AS DOUBLE) AS open_price,
                    CAST("2. high" AS DOUBLE) AS high_price,
                    CAST("3. low" AS DOUBLE) AS low_price,
                    CAST("4. close" AS DOUBLE) AS close_price,
                    CAST("5. volume" AS DOUBLE) AS trading_volume,
                    CURRENT_TIMESTAMP AS processed_dttm
                FROM {tmp_table_name}
                ORDER BY dt DESC
            ) TO '{self.schematable_path_stg}' (
                FORMAT PARQUET, 
                COMPRESSION ZSTD
            )
            ;
        """)

    def load_from_stg_to_target(self) -> None:
        self.conn.execute(f"""
            INSERT INTO {self.schematable_path}
            SELECT *
            FROM read_parquet({self.schematable_path_stg});
        """)

    def process(self) -> int:
        res_json = self.fetch_asset_data_json()

        DuckdbUtils.clean_table(self.schematable_path_stg, self.conn)
        self.load_json_to_stg(res_json)
        row_cnt = DuckdbUtils.check_table_data(self.schematable_path_stg, self.conn)

        if row_cnt > 0:
            DuckdbUtils.clean_table(self.schematable_path, self.conn)
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
