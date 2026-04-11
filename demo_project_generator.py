import os
from pathlib import Path

def create_test_project():
    # Define the base directory
    base_dir = Path("test_project")
    
    # 1. Define Structure using Path objects for robustness
    dirs = [
        base_dir / 'src', 
        base_dir / 'src/utils', 
        base_dir / 'logs'
    ]
    
    files = {
        base_dir / 'src/main.py': """
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
""",
        base_dir / 'src/utils/db.py': """
def connect_db():
    # Intentional bug for debugging test
    # Matches the 5XX errors found in the logs
    raise ConnectionError("Database cluster 'db-01' is unreachable.")
""",
        base_dir / 'logs/2026-04-10.text': """
2026-04-10_10:00:01 || main.py || 5 || INFO: System Bootstrapped
2026-04-10_12:30:45 || db.py || 4 || 500 Server Error: Internal Server Error for url: http://internal-api/v1/connect
2026-04-10_12:31:02 || db.py || 4 || 503 Service Unavailable: Database cluster overloaded
2026-04-10_15:00:00 || main.py || 10 || INFO: Health check passed
""",
        base_dir / 'logs/2026-04-11.text': """
2026-04-11_08:15:22 || db.py || 4 || 502 Bad Gateway: Upstream server timed out
2026-04-11_09:00:01 || main.py || 5 || INFO: Restarting after crash
2026-04-11_09:05:44 || db.py || 4 || 500 Internal Server Error: Null pointer at DBConnector.java:88
2026-04-11_10:10:10 || main.py || 12 || ERROR: Persistent 5XX errors detected in downstream services
"""
    }

    # 2. Create Base and Sub-Directories
    print(f"🚀 Initializing test project in: {base_dir.resolve()}\n")
    for d in dirs:
        d.mkdir(parents=True, exist_ok=True)
        print(f"  [DIR]  Created: {d}")

    # 3. Create Files
    for path, content in files.items():
        with open(path, 'w', encoding='utf-8') as f:
            f.write(content.strip())
        print(f"  [FILE] Created: {path}")

    # 4. Final Summary
    print("\n✅ Test Project 'test_project' is ready for debugging!")
    print("-" * 50)
    print("Project Tree:")
    print(f"  {base_dir}/")
    print("  ├── src/")
    print("  │   ├── main.py")
    print("  │   └── utils/")
    print("  │       └── db.py")
    print("  └── logs/")
    print("      ├── 2026-04-10.text")
    print("      └── 2026-04-11.text")
    print("-" * 50)

if __name__ == "__main__":
    create_test_project()