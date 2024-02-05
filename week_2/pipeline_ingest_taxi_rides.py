#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pyarrow
import pandas as pd
from sqlalchemy import create_engine, inspect
import pyarrow.parquet as pq

# Arguments

arg_dict = {
    "dialect" : "Dialect e.g. postgresql, mysql, etc.", # keeps database neutral
    "user" : "db user",
    "password": "db user password",
    "host" : "database host url",
    "port" : "database port",
    "database" : "database name",
    "table_name" : "table where result will be written",
    "file_to_ingest" : "url of the file to ingest"
}

parser = argparse.ArgumentParser(description='Params for ingestion script')

for arg, help_string in arg_dict.items():
    parser.add_argument(arg, help=help_string)

class IngestTaxiRidesData:
    def __init__(self, params) -> None:
        self.file_url = params.file_to_ingest
        self.file_name = "output.csv"
        self.__table_name = params.table_name
        self.__database_uri = f"{params.dialect}://{params.user}:{params.password}@{params.host}:{params.port}/{params.database}"

    def __connect_to_db(self) -> None:
        self.__engine = create_engine(self.__database_uri)
        self.__connection = self.__engine.connect()

    def __ingest_parquet(self, file_name):
        """
        Ingest parquet files into database
        """
        parquet_file = pq.ParquetFile(file_name)
        for i in parquet_file.iter_batches(batch_size=200000):
            pd_df = i.to_pandas()
            pd_df.tpep_dropoff_datetime = pd.to_datetime(pd_df.tpep_dropoff_datetime)
            pd_df.tpep_pickup_datetime = pd.to_datetime(pd_df.tpep_pickup_datetime)
            pd_df.to_sql(self.__table_name, con=self.__engine, if_exists='append')
        

    def __ingest_csv(self, file_name):
        """
        Ingest csv files into database
        """
        df = pd.read_csv(file_name, converters={"tpep_pickup_datetime":pd.Timestamp, "tpep_dropoff_datetime" : pd.Timestamp},
                          iterator=True, batch_size=100000)


        pass

    def _download_source_file(self):
        """
        Downloads and validates the source file
        """
        # Download source file

        if "parquet" == self.file_url.rsplit(".", 1)[1]:
            self.file_name == "output.parquet"

        os.system(f"wget -O {self.file_url} {self.file_name}")

        return os.path.exists(self.file_name) and os.stat(self.file_name).st_size
    
    def __create_table(self):
        """
        Creates table if not exists
        """
        df_iter = pd.read_csv(self.file_name, converters={"tpep_pickup_datetime":pd.Timestamp, "tpep_dropoff_datetime" : pd.Timestamp},
                          iterator=True, batch_size=1)
        df = next(df_iter)
        df.head(n=0).to_sql(name=self.__table_name, con=self.__engine, if_exists='replace')

    def ingest_data_to_db(self):
        """
        Main function to drive the 
        """
        # Download and validate file

        is_downloaded = self._download_source_file()

        # Open connection to db

        if is_downloaded:
            self.__connect_to_db()

            # If connect successful, store data atomically

            inspector = inspect(self.__connection)
            table_exists = inspector.has_table(self.__table_name)

            if not table_exists:
                self.__create_table()

            self.__ingest_csv()

            
        pass

def main():
    params = parser.parse_args()

    ingest = IngestTaxiRidesData(params)
    ingest.ingest_data_to_db()


# engine = create_engine("postgresql://root:root@localhost:5432/ny_taxi")
# engine.connect()
#
# zone = pd.read_csv("taxi_zone_lookup.csv")
#
# taxi_zones.head(n=0).to_sql(name='green_taxi_trips', con=engine, if_exists='replace')
#
# while True:
#     df = next(taxi_data_iter)
#     df.to_sql(name='green_taxi_trips', con=engine, if_exists='append')
#     print("Chunk inserted")
