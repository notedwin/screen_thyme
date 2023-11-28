from datetime import datetime
from sqlite3 import connect
from typing import ClassVar

import pandas as pd
import structlog
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    MetaData,
    Select,
    Table,
    create_engine,
    func,
)
from sqlalchemy.types import DateTime, Integer

log = structlog.get_logger()


class PostgresResource(BaseModel):
    url: str
    _con = None
    _metadata = MetaData()

    def __enter__(self):
        self._con = create_engine(self.url).connect()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self._con.commit()
        self._con.close()

    def table_exists(self, table_name: str):
        log.info(f"Looking for {table_name}")
        try:
            self._metadata.reflect(bind=self._con)
            self._metadata.tables[table_name]
            return True
        except KeyError:
            return False

    def get_max_row_num(self, table_name: str):
        metadata_table = table_name + "_metadata"
        tbl = Table(
            metadata_table,
            MetaData(),
            Column("last_row", Integer),
            Column("date", DateTime),
        )

        if self.table_exists(metadata_table):
            tbl = Table(metadata_table, self._metadata)
            # f"SELECT MAX(last_row) AS num_row FROM {metadata_table}"
            stmt = Select(func.max(tbl.c.last_row))
            row = self._con.execute(stmt).fetchone()
            log.info(f"get_max_row_num: {row}")
            if row is not None and row[0] is not None:
                return row[0]
        return 0

    def insert_new_rows(self, table_name: str, last_row: int):
        date = datetime.today().strftime("%Y-%m-%d %H:%M:%S")
        metadata_table = table_name + "_metadata"
        tbl = Table(
            metadata_table,
            MetaData(),
            Column("last_row", Integer),
            Column("date", DateTime),
        )
        if not self.table_exists(metadata_table):
            tbl.create(bind=self._con)

        stmt = tbl.insert().values(last_row=last_row, date=date)
        self._con.execute(stmt)

    def insert_df(self, df: pd.DataFrame, table_name):
        df.to_sql(
            table_name,
            con=self._con,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi",
        )

    def insert_df_update(
        self, df: pd.DataFrame, table_name: str, pk: str = "id"
    ) -> int:
        self.insert_df(df, table_name)
        last_processed = df[pk].max() if df.shape[0] > 0 else 0
        self.insert_new_rows(table_name, int(last_processed))
        return int(last_processed)


class SQLiteResource(BaseModel):
    path: str
    connection: ClassVar = None

    def __enter__(self):
        SQLiteResource.connection = connect(self.path)
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        SQLiteResource.connection.close()

    def execute_query(self, sql: str):
        return pd.read_sql_query(sql, con=self.connection)
