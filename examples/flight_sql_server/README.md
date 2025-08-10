# Query Liquid Cache with Python DBAPI using Arrow Flight SQL (ADBC)

This example demonstrates how to use `liquid-cache` as a high-performance data caching layer and query it directly from Python using the standard DBAPI interface. By leveraging Arrow Flight SQL (ADBC), you can execute complex SQL queries against datasets (like Parquet files) and get results back as Polars or Pandas DataFrames, benefiting from `liquid-cache`'s caching to accelerate subsequent queries.

The entire setup is containerized using Docker Compose for easy and reproducible deployment.

## Running Docker

The easiest way to get the server running is with Docker Compose. This repository includes the necessary `Dockerfile` and `docker-compose.yml` files.

### Configuration

The Docker Compose setup uses a shared volume to make a local directory available to the containers for reading data files (like Parquet files).

By default, this setup maps the `/tmp/data` directory on your host machine to `/tmp/data` inside the containers. You can customize the host directory by creating a `.env` file in this directory (`flight_sql_server`) with the following content:

```
DATA_PATH=/path/to/your/data
```

Replace `/path/to/your/data` with the absolute path to the directory containing your data on the host machine. For the example client to work, you would place `hits.parquet` inside this directory.

1.  **Start the server:**

    From the `flight_sql_server` directory, run:
    ```bash
    docker-compose up --build
    ```

    This will build the Docker image and start the Flight SQL server, which will listen on `localhost:15215`.

2.  **Connect to the server:**

    You can now connect using the Python client described below.

## Connecting with a Python ADBC Client

You can connect to the server using any ADBC-compatible client. Here is an example using the Python ADBC Flight SQL driver.

### Prerequisites

First, install the necessary Python packages:

```bash
pip install adbc_driver_flightsql pyarrow polars
```

### Example Client

Create a Python file (e.g., `client.py`) with the following content:

```python
import adbc_driver_flightsql.dbapi as flight_sql
import polars as pl

# The URI for the Flight SQL server.
# Use port 15215 for Docker, and 50051 for a native build.
uri = "grpc://localhost:15215"
print(f"Connecting to {uri}...")

try:
    with flight_sql.connect(uri) as conn:
        with conn.cursor() as cur:

            # Add file to liquid-cache. The path '/data/hits.parquet' corresponds to the volume mount in the container.
            create_table_query = f"""
                CREATE EXTERNAL TABLE hits
                STORED AS PARQUET
                LOCATION 'file:///data/hits.parquet';
            """
            cur.execute(create_table_query)
            
            # Run query 
            query = """
                SELECT 
                    "SearchPhrase", 
                    MIN("URL"), 
                    MIN("Title"), 
                    COUNT(*) AS c, 
                    COUNT(DISTINCT "UserID") 
                FROM 
                    hits 
                WHERE 
                    "Title" LIKE '%Google%' 
                    AND "URL" NOT LIKE '%.google.%' 
                    AND "SearchPhrase" <> '' 
                GROUP BY "SearchPhrase" 
                ORDER BY c DESC 
                LIMIT 10;
            """
            print(f"Executing query: {query}")
            cur.execute(query)

            # Fetch results as a Polars DataFrame ( for pandas use cur.fetch_df)
            df = cur.fetch_polars()

            print("Query result:")
            print(df)

except Exception as e:
    print(f"An error occurred: {e}")

```

### Running the Client

Execute the Python script from your terminal:

```bash
python client.py
```

You should see a successful connection and query result.
Try to run the same query more time and measure the run time reduction