


from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware

# FastAPI app instance
# app = FastAPI()
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
    'password': 'KIESQUARE123'  # Keep this secure
}

# Request model for the payload
class QueryParams(BaseModel):
    project_id: int
    brandname: str

# Function to get data from the database
def get_data_from_db():
    try:
        connection = psycopg2.connect(**DB_PARAMS)
        query = "SELECT * FROM public.normalisedvalue;"
        df = pd.read_sql(query, connection)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching data from the database: {e}")
    finally:
        if connection:
            connection.close()
    return df

@app.post("/top_5/")
def get_metrics(params: QueryParams):
    df = get_data_from_db()
    
    # Filter by project_id and brandname from the payload
    df_filtered = df[(df['project_id'] == params.project_id) & (df['brandname'] == params.brandname)]
    
    # Drop duplicates based on metricid
    df_filtered = df_filtered.drop_duplicates(subset=['metricid'])

    if df_filtered.empty:
        raise HTTPException(status_code=404, detail="No data found for the provided project_id and brandname.")

    # Get top 5 and bottom 5 normalized values for each section
    top_metrics = (
        df_filtered.groupby('sectionname')
        .apply(lambda x: x.nlargest(5, 'normalized')[['sectionname', 'platformname','metricname', 'metricid', 'normalized', 'weights']])
        .reset_index(drop=True)
        .to_dict(orient='records')
    )

    bottom_metrics = (
        df_filtered.groupby('sectionname')
        .apply(lambda x: x.nsmallest(5, 'normalized')[['sectionname','platformname', 'metricname', 'metricid', 'normalized', 'weights']])
        .reset_index(drop=True)
        .to_dict(orient='records')
    )

    return {
        "top_metrics": top_metrics,
        "bottom_metrics": bottom_metrics
    }

# Run the app using Uvicorn
# Command to run: uvicorn <filename_without_py>:app --reload
