

def get_env_mode(default="livetest"):
    """load .env values if exist and get current MODE.
    Available Modes: live, livetest, backtest, simulation"""
    from dotenv import load_dotenv
    from os import getenv
    load_dotenv()
    return getenv("MODE", default)
