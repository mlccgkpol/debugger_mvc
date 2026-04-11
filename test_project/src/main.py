import logging
from utils.db import connect_db

def start_app():
    print("Starting Application...")
    try:
        connect_db()
    except Exception as e:
        print(f"App Crash: {e}")

if __name__ == "__main__":
    start_app()