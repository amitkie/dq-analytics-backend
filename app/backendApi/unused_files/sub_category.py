from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql

# FastAPI app instance
# app = FastAPI()
from fastapi.middleware.cors import CORSMiddleware
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware

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

# Pydantic model for the brand information
class BrandInfo(BaseModel):
    brand: str
    category: str
    sub_category: str

# Pydantic model for the response including competitors
class BrandResponse(BaseModel):
    main_brand: BrandInfo
    competitors: list[BrandInfo]

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_PARAMS)

@app.get("/brands/{brand_name}", response_model=BrandResponse)
async def get_brand_info(brand_name: str):
    """Retrieve information for a specific brand and additional brands in the same category as competitors."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # First, find the category and sub-category for the given brand
        query = sql.SQL("""
            SELECT category, sub_category 
            FROM dq.master_table_category_brand 
            WHERE brand = %s
        """)
        
        cursor.execute(query, (brand_name,))
        brand_result = cursor.fetchone()
        
        if not brand_result:
            raise HTTPException(status_code=404, detail="Brand not found")
        
        category, sub_category = brand_result
        
        # Now, find all brands in the same category (excluding the main brand)
        query_all = sql.SQL("""
            SELECT brand, category, sub_category 
            FROM dq.master_table_category_brand 
            WHERE category = %s AND brand != %s
        """)
        
        cursor.execute(query_all, (category, brand_name))
        competitor_brands = cursor.fetchall()
        
        # Construct the main brand info
        main_brand_info = BrandInfo(brand=brand_name, category=category, sub_category=sub_category)
        
        # Construct the competitors list
        competitors = [BrandInfo(brand=row[0], category=row[1], sub_category=row[2]) for row in competitor_brands]
        
        return BrandResponse(main_brand=main_brand_info, competitors=competitors)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cursor.close()
        conn.close()

