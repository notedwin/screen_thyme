import os

from dotenv import find_dotenv, load_dotenv

from screen_thyme import LaunchdManager

# provide secrets in .env file or hardcode
load_dotenv(find_dotenv())
APPLE_PATH = os.getenv(key="APPLE_PATH")
AW_PATH = os.getenv(key="AW_PATH")
PG_URL = os.getenv(key="PG_URL")


if __name__ == "__main__":
    env_vars = {"APPLE_PATH": APPLE_PATH, "AW_PATH": AW_PATH, "PG_URL": PG_URL}

    m = LaunchdManager(
        env_vars=env_vars,
        path=os.path.dirname(os.path.realpath(__file__)),
    )
    m.create_plist()
    m.unload_job()
    m.load_job()
