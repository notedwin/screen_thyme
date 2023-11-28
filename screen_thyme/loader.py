import logging
import os
import plistlib
import subprocess

import structlog
from pydantic import BaseModel

from .connections import PostgresResource, SQLiteResource

structlog.configure(
    wrapper_class=structlog.make_filtering_bound_logger(logging.INFO),
)


log = structlog.get_logger()


class ScreenThyme(BaseModel):
    postgres_url: str
    aw_path: str = None
    apple_path: str = None

    def export(self):
        # add a way to export one

        self.apple_exporter()
        self.aw_exporter()

    def apple_exporter(self):
        table_name = "zobject"
        with PostgresResource(url=self.postgres_url) as pg:
            max_row_num = pg.get_max_row_num(table_name)
            with SQLiteResource(path=self.apple_path) as sqlite:
                df = sqlite.execute_query(
                    f"SELECT * FROM {table_name} WHERE Z_PK > {max_row_num}"
                )
                pg.insert_df_update(df, table_name, pk="Z_PK")

    def aw_exporter(self):
        table_name = "eventmodel"
        with PostgresResource(url=self.postgres_url) as pg:
            max_row_num = pg.get_max_row_num(table_name)
            with SQLiteResource(path=self.aw_path) as sqlite:
                df = sqlite.execute_query(
                    f"SELECT * FROM {table_name} WHERE id > {max_row_num}"
                )
                pg.insert_df_update(df, table_name)


class LaunchdManager(BaseModel):
    env_vars: dict = None
    path: str

    def load_job(self):
        plist_file = os.path.join(self.path, "io.screen.thyme.plist")
        command = f"launchctl load -w {plist_file}"
        subprocess.run(command, shell=True)

    def unload_job(self):
        plist_file = os.path.join(self.path, "io.screen.thyme.plist")
        command = f"launchctl unload -w {plist_file}"
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
import screen_thyme.loader as st

# loaded in from plist file
APPLE_PATH = os.getenv(key="APPLE_PATH")
AW_PATH = os.getenv(key="AW_PATH")
PG_URL = os.getenv(key="PG_URL")

st.ScreenThyme(postgres_url=PG_URL, aw_path=AW_PATH, apple_path=APPLE_PATH).export()
"""
        file_ = os.path.join(self.path, "runner.py")
        with open(file_, "w") as fp:
            fp.write(script)
