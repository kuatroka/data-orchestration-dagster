import duckdb
conn = duckdb.connect(database="/Users/yo_macbook/Documents/dev/data-orchestration-dagster/dagster_university/data/staging/data.duckdb")
conn.execute("drop table trips;")
