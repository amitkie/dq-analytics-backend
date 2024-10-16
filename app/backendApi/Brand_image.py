
import psycopg2
from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from io import BytesIO

# Database connection parameters
DB_PARAMS = {
    'host': 'detool.cq7xabbes0x8.ap-south-1.rds.amazonaws.com',
    'port': '5434',
    'dbname': 'KIESQUAREDE',
    'user': 'KIESQUAREDE',
    'password': 'KIESQUARE123'
}
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


def get_db_connection():
    """Establish a database connection."""
    return psycopg2.connect(**DB_PARAMS)

@app.get("/brand-images/{brand_name}")
async def retrieve_brand_images(brand_name: str):
    """
    Retrieve images for a specific brand from the database.
    """
    try:
        conn = get_db_connection()
        cur = conn.cursor()

        # Query to fetch brand images based on brand name
        cur.execute("SELECT image FROM dq.master_table_brand_images WHERE brand = %s", (brand_name,))
        rows = cur.fetchall()

        if not rows:
            raise HTTPException(status_code=404, detail="Brand not found or no images available.")

        # Return the first image as a streaming response
        image_bytes = rows[0][0]  # Get the first image byte data
        image_stream = BytesIO(image_bytes)
        return StreamingResponse(image_stream, media_type="image/jpeg")

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    finally:
        # Close the cursor and connection
        if cur:
            cur.close()
        if conn:
            conn.close()

#GET 
# http://127.0.0.1:8013/brand-images/Livon