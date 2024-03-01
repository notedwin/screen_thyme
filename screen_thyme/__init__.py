import logging
import os
import plistlib
import subprocess

import duckdb
import structlog
from pydantic import BaseModel

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)

log = structlog.get_logger()


class ScreenThyme(BaseModel):
    postgres_url: str
    aw_path: str = None
    apple_path: str = None

    def export(self):
        if not self.postgres_url:
            return Exception("No pg url given")

        duckdb.execute(
            f"""--sql
            INSTALL postgres; LOAD postgres;
            INSTALL sqlite; LOAD sqlite;
            SET GLOBAL sqlite_all_varchar = true;
            ATTACH 'postgres:{self.postgres_url}' as postgres
        """
        )

        if self.apple_path:
            duckdb.execute(f"ATTACH 'sqlite:{self.apple_path}' AS apple;")
            self.apple_exporter()
        if self.aw_path:
            duckdb.execute(f"ATTACH 'sqlite:{self.aw_path}' AS aw;")
            self.aw_exporter()

    def apple_exporter(self):
        """
        flow:
            get last row seen from postgres
            pull all data after last row from sqlite and insert into postgres
            update metadata with new last row

        Notes:
        When using postgres ext, it requires you use SQL syntax for it.
        ex. NOW() is only available in postgres and not duckdb
        """

        latest_row = "SELECT MAX(last_row) FROM postgres.apple_screentime_metadata;"
        last_row = duckdb.execute(latest_row).fetchone()[0]
        print(f"adding rows after {last_row}")

        insert_new = f"""--sql
        INSERT INTO postgres.apple_screentime
        SELECT
        Z_PK as z_pk,
        ZSTREAMNAME as zstreamname,
        ZVALUESTRING as zvaluestring,
        to_timestamp(ZCREATIONDATE::DOUBLE + 978307200)  AS zcreationdate,
        to_timestamp(ZENDDATE::DOUBLE + 978307200) AS zenddate,
        to_timestamp(ZLOCALCREATIONDATE::DOUBLE + 978307200) AS zlocalcreationdate,
        to_timestamp(ZSTARTDATE::DOUBLE + 978307200) AS zstartdate
        FROM apple.zobject
        WHERE Z_PK > {last_row}
        """

        duckdb.execute(insert_new)

        rows = duckdb.execute(
            f"SELECT COUNT(1) FROM apple.zobject WHERE Z_PK > {last_row} LIMIT 1"
        ).fetchone()[0]
        print(f"Inserted {rows} new rows!")

        update_metadata = """--sql
        INSERT INTO postgres.apple_screentime_metadata
        SELECT MAX(Z_PK::INT) AS last_row,
        NOW() as date
        FROM apple.zobject
        """
        duckdb.execute(update_metadata)

        last_row = duckdb.execute(
            "SELECT MAX(Z_PK::INT) FROM apple.zobject"
        ).fetchone()[0]
        print(f"Last row seen {last_row}, Metadata updated!")

    def aw_exporter(self):
        latest_row = "SELECT MAX(last_row) FROM postgres.eventmodel_metadata;"
        last_row = duckdb.execute(latest_row).fetchone()[0]
        print(f"adding rows after {last_row}")

        insert_new = f"""--sql
        INSERT INTO postgres.eventmodel
        SELECT *
        FROM aw.eventmodel
        WHERE id > {last_row}::int
        """
        duckdb.execute(insert_new)

        update_metadata = """--sql
        INSERT INTO postgres.eventmodel_metadata
        SELECT MAX(id::int) AS last_row,
        NOW() as date
        FROM aw.eventmodel
        """

        duckdb.execute(update_metadata)

        last_row = duckdb.execute("SELECT MAX(id::int) FROM aw.eventmodel").fetchone()[
            0
        ]
        print(f"Last row seen {last_row}, Metadata updated!")


class LaunchdManager(BaseModel):
    env_vars: dict = None
    path: str

    def load_job(self):
        command = f"cp {os.path.join(self.path, 'io.screen.thyme.plist')} ~/Library/LaunchAgents/io.screen.thyme.plist"
        subprocess.run(command, shell=True)
        command = "launchctl load -w ~/Library/LaunchAgents/io.screen.thyme.plist"
        subprocess.run(command, shell=True)

    def unload_job(self):
        command = "launchctl unload -w ~/Library/LaunchAgents/io.screen.thyme.plist"
        subprocess.run(command, shell=True)

    def create_plist(self):
        self.create_script()
        script_location = os.path.join(self.path, "runner.py")

        python_loc = None

        try:
            python_loc = subprocess.check_output(
                "pyenv which python3", shell=True
            ).decode("UTF-8")
        except subprocess.CalledProcessError:
            python_loc = subprocess.check_output("which python3", shell=True).decode(
                "UTF-8"
            )

        python_loc = python_loc.strip("\n")

        d = {
            "Label": "io.screen.thyme",
            "ProgramArguments": [python_loc, script_location],
            # ["/usr/bin/open", "-W", "/Applications/Calculator.app"],
            "EnvironmentVariables": self.env_vars,
            "RunAtLoad": True,
            "KeepAlive": True,
            "StartOnMount": True,
            "StartInterval": 3600,
            "ThrottleInterval": 3600,
            # https://apple.stackexchange.com/questions/435496/launchd-service-logs
            "StandardErrorPath": "/tmp/local.job.err",
            "StandardOutPath": "/tmp/local.job.out",
        }

        file_ = os.path.join(self.path, "io.screen.thyme.plist")
        with open(file_, "wb+") as fp:
            plistlib.dump(d, fp)

    def create_script(self):
        script = """import os
import screen_thyme as st

# loaded in from plist file
APPLE_PATH = os.getenv(key="APPLE_PATH")
AW_PATH = os.getenv(key="AW_PATH")
PG_URL = os.getenv(key="PG_URL")

st.ScreenThyme(postgres_url=PG_URL, aw_path=AW_PATH, apple_path=APPLE_PATH).export()
"""
        file_ = os.path.join(self.path, "runner.py")
        with open(file_, "w") as fp:
            fp.write(script)
