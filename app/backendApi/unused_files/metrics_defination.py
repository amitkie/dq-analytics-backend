from fastapi import FastAPI, HTTPException, Query
import psycopg2
from psycopg2 import sql
from typing import Dict
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware

from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

# Add CORS middleware
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://example.com",
    "*",
    "(*)"
]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.add_middleware(BearerTokenMiddleware)

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}

# Database connection function
def get_db_connection():
    conn = psycopg2.connect(
        host=DB_PARAMS['host'],
        port=DB_PARAMS['port'],
        dbname=DB_PARAMS['dbname'],
        user=DB_PARAMS['user'],
        password=DB_PARAMS['password']
    )
    return conn


@app.get("/definition/", response_model=Dict[str, str])
async def get_metric_definition(
    platform_name: str = Query(...), 
    metric_name: str = Query(...)
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Updated SQL query to filter by both platform and metrics
        query = sql.SQL(
            'SELECT definition FROM dq.master_table_platform_metrics_relationship_new WHERE platform = %s AND metrics = %s;'
        )
        cursor.execute(query, (platform_name, metric_name))
        row = cursor.fetchone()

        if row:
            return {"definition": row[0]}
        else:
            raise HTTPException(status_code=404, detail="Metric not found")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

# To run the application, use:
# uvicorn your_file_name:app --reload
# GET
# https://dndrvx80-8014.inc1.devtunnels.ms/definition/?platform_name=Amazon - Display Campaigns&metric_name=ACOS %