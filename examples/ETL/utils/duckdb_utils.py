import duckdb
from duckdb import DuckDBPyConnection


CATALOG_NAME = 'iceberg'
NESSIE_CATALOG_ENDPOINT = 'http://nessie-catalog:19120/iceberg/'
S3_ENDPOINT = 'ozone-s3g:9878'
S3_ACCESS_KEY = 'ozone-access-key'
S3_SECRET_KEY = 'ozone-secret-key'


class DuckdbUtils:

    @staticmethod
    def setup_db() -> DuckDBPyConnection:
        return duckdb.connect()

    @staticmethod
    def setup_catalog(duckdb_connection: DuckDBPyConnection) -> DuckDBPyConnection:
        duckdb_connection.execute(f"""
            CREATE OR REPLACE SECRET s3_secret (
                TYPE s3,
                ENDPOINT '{S3_ENDPOINT}',
                KEY_ID '{S3_ACCESS_KEY}',
                SECRET '{S3_SECRET_KEY}',
                USE_SSL false,
                URL_STYLE 'path',
                REGION 'us-east-1'
            );
        """)
        
        duckdb_connection.execute(f"""
            ATTACH 'warehouse' AS {CATALOG_NAME} (
                TYPE iceberg,
                AUTHORIZATION_TYPE 'NONE',
                ENDPOINT '{NESSIE_CATALOG_ENDPOINT}'
            );
        """)

        duckdb_connection.execute(f"""
            SET s3_endpoint='{S3_ENDPOINT}';
            SET s3_access_key_id='{S3_ACCESS_KEY}';
            SET s3_secret_access_key='{S3_SECRET_KEY}';
            SET s3_use_ssl=false;
            SET s3_url_style='path';
            SET s3_region='us-east-1';
        """)
        return duckdb_connection, CATALOG_NAME

    @staticmethod
    def clean_path(schematable_to_clean: str, duckdb_connection: DuckDBPyConnection) -> None:
        duckdb_connection.execute(f"""
            TRUNCATE TABLE {schematable_to_clean};
        """)

    @staticmethod
    def check_table_path(schematable_to_check: str, duckdb_connection: DuckDBPyConnection) -> int:
        cnt = duckdb_connection.execute(f"""
            SELECT COUNT(*) FROM read_parquet({schematable_to_check});
        """).fetchone()[0]
        return cnt
