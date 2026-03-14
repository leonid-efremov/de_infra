class PySparkDatabase:

    def __init__(self, spark_session, catalog_name, catalog_location):
        self.spark = spark_session
        self.catalog_name = catalog_name
        self.catalog_location = catalog_location

    def create_database(self, db_name):
        self.spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {self.catalog_name}.{db_name}
            LOCATION '{self.catalog_location}/{db_name}'
        """)

    def create_table(self, table_data, db_list):
        databases = [db_name for db_name in db_list if table_data['layer'] in db_name]
        partitioning = '' if table_data['partition'] is None else f"PARTITIONED BY ({','.join(table_data['partition'])})"

        table_name = lambda db: f"{self.catalog_name}.{db}.{table_data['name']}"
        table_location = lambda db: f"{self.catalog_location}/{db}/{table_data['name']}"

        for db_name in databases:
            self.spark.sql(f"""
                DROP TABLE IF EXISTS {table_name(db_name)};
            """)
            self.spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {table_name(db_name)} (
                    {table_data['attrs']}
                )
                USING iceberg
                TBLPROPERTIES (
                    'format-version' = '2',
                    'write.parquet.compression-codec' = 'snappy'                
                )
                {partitioning}
                LOCATION '{table_location(db_name)}';
            """)

    def show_all_tables(self):
        schemas = [row[0] for row in self.spark.sql(f"SHOW DATABASES IN {self.catalog_name}").collect()]
        # Для каждой схемы выводим таблицы
        for schema in schemas:
            tables = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}.{schema}").collect()
            for table in tables:
                print(f"  - {schema}.{table.tableName}")


class PySparkIcebergUtils:
    """
    CALL iceberg_catalog.system.expire_snapshots('db.sample', TIMESTAMP '2021-06-30 00:00:00.000', 100);
    CALL iceberg_catalog.system.remove_orphan_files(table => 'db.sample', location => 'tablelocation/data');
    """
    pass


class PySparkTableLoader:

    def __init__(self, table_name, stg_table_name, partition_by, spark_session):
        self.table_name = table_name
        self.stg_table_name = stg_table_name
        self.partition_by = partition_by
        self.spark = spark_session

    def _get_dataframe_writer(self, df_to_write, table_to_write):
        df_writer = df_to_write.writeTo(table_to_write)
        if self.partition_by:
            df_writer = df_writer.partitionedBy(*self.partition_by)
        df_final_writer = df_writer \
            .tableProperty('write.distribution-mode', 'none') \
            .tableProperty('write.write-format', 'parquet') \
            .tableProperty('write.compression-codec', 'snappy')
        return df_final_writer

    def calc_stg(self, stg_sql):
        df = self.spark.sql(stg_sql)
        df_writer = self._get_dataframe_writer(df, self.stg_table_name) 

        df_writer.createOrReplace()
        return self

    def load_trg_scd1(self, overwrite_trg=False):
        df = self.spark.table(self.stg_table_name)
        df_writer = self._get_dataframe_writer(df, self.table_name) 

        if overwrite_trg:
            df_writer.createOrReplace()
        else:
            df_writer.overwritePartitions()
        return self
