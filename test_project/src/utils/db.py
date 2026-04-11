def connect_db():
    # Intentional bug for debugging test
    # Matches the 5XX errors found in the logs
    raise ConnectionError("Database cluster 'db-01' is unreachable.")