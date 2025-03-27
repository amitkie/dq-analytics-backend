from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from fastapi import APIRouter, HTTPException, Query
from typing import Dict
from pydantic import BaseModel
import pandas as pd
from fastapi.responses import StreamingResponse
from io import BytesIO
from app.backendApi.config.db import DB_PARAMS

# FastAPI app instance
app = APIRouter()

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
        cur.execute("SELECT image FROM dq.master_table_category_brand WHERE brand = %s", (brand_name,))
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
####################################################################################




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
###################################################################

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
    metrics_id: str = Query(...)
):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Updated SQL query to filter by both platform and metrics
        query = sql.SQL(
            'SELECT definition FROM dq.master_table_platform_metrics_relationship_new WHERE platform = %s AND metrics_id = %s;'
        )
        cursor.execute(query, (platform_name, metrics_id))
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

############################
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
        .apply(lambda x: x.nlargest(5, 'normalized')[['sectionname','platformname', 'metricname', 'metricid', 'normalized', 'weights']])
        .reset_index(drop=True)
        .to_dict(orient='records')
    )

    bottom_metrics = (
        df_filtered.groupby('sectionname')
        .apply(lambda x: x.nsmallest(5, 'normalized')[['sectionname','platformname',  'metricname', 'metricid', 'normalized', 'weights']])
        .reset_index(drop=True)
        .to_dict(orient='records')
    )

    return {
        "top_metrics": top_metrics,
        "bottom_metrics": bottom_metrics
    }


#Brand_image metrics_defination and sub_category combined

#python3 -m uvicorn brand_sub_image_defination:app --reload --port 8011
#GET
#https://hrsbjqs8-8011.inc1.devtunnels.ms/brand-images/Livon
#https://hrsbjqs8-8011.inc1.devtunnels.ms/brands/Livon
#https://hrsbjqs8-8011.inc1.devtunnels.ms/definition/?platform_name=Amazon - Display Campaigns&metric_name=ACOS %



from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import psycopg2
from psycopg2 import sql
from fastapi.middleware.cors import CORSMiddleware


import psycopg2
import pandas as pd
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List, Dict
from fastapi.middleware.cors import CORSMiddleware
# Pydantic model to validate request input
class BrandQuery(BaseModel):
    project_id: int
    brand_name: str

# Helper function to fetch and filter the data
def fetch_filtered_data(project_id: int, brand_name_input: str) -> Dict[str, List[Dict]]:
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()

        # Define the query
        query = f'SELECT * FROM public."userProjectDQScores";'

        # Execute the query
        cur.execute(query)

        # Fetch all results from the executed query
        data = cur.fetchall()

        # Get column names
        column_names = [desc[0] for desc in cur.description]

        # Convert results to a pandas DataFrame
        df = pd.DataFrame(data, columns=column_names)

        # Close the cursor and connection
        cur.close()
        conn.close()
        # Check if the project_id exists in the data
        if project_id not in df['project_id'].values:
            return {"message": f"project_id {project_id} is not present in userProjectDQScores"}


        # Filter DataFrame based on project_id
        df = df[df['project_id'] == project_id]

        # Drop duplicates based on category_name and brand_id
        #df = df.drop_duplicates(subset=['category_name', 'brand_id'])
        df = df.drop_duplicates(subset=[col for col in df.columns if col not in ['createdAt', 'updatedAt', 'id']])
        if brand_name_input not in df['brand_name'].values:
            return {"message": f"Brand name '{brand_name_input}' not found in the data."}


        # Find the category_name for the given brand_name
        if brand_name_input not in df['brand_name'].values:
            raise ValueError(f"Brand name '{brand_name_input}' not found in the data.")

        category_name_for_brand = df[df['brand_name'] == brand_name_input]['category_name'].iloc[0]

        # Filter the DataFrame by category_name
        filtered_df = df[df['category_name'] == category_name_for_brand]
        filtered_df = filtered_df[['project_id', 'brand_name', 'section_name', 'dq', 'ecom_dq', 'social_dq', 'paid_dq', 'brand_perf_dq']]

        # Replace NaN values with None (which is compatible with JSON)
        filtered_df = filtered_df.applymap(lambda x: None if pd.isna(x) or x == float('inf') or x == float('-inf') else x)

        # Calculate statistics: average, median (50th percentile), and 75th percentile
        statistics = {
            "dq": {
                "average": filtered_df['dq'].mean(),
                "50th_percentile": filtered_df['dq'].median(),
                "75th_percentile": filtered_df['dq'].quantile(0.75)
            },
            "ecom_dq": {
                "average": filtered_df['ecom_dq'].mean(),
                "50th_percentile": filtered_df['ecom_dq'].median(),
                "75th_percentile": filtered_df['ecom_dq'].quantile(0.75)
            },
            "social_dq": {
                "average": filtered_df['social_dq'].mean(),
                "50th_percentile": filtered_df['social_dq'].median(),
                "75th_percentile": filtered_df['social_dq'].quantile(0.75)
            },
            "paid_dq": {
                "average": filtered_df['paid_dq'].mean(),
                "50th_percentile": filtered_df['paid_dq'].median(),
                "75th_percentile": filtered_df['paid_dq'].quantile(0.75)
            },
            "brand_perf_dq": {
                "average": filtered_df['brand_perf_dq'].mean(),
                "50th_percentile": filtered_df['brand_perf_dq'].median(),
                "75th_percentile": filtered_df['brand_perf_dq'].quantile(0.75)
            }
        }

        # Sort each column independently
        result = {
            "dq": sorted(filtered_df[['project_id', 'brand_name', 'section_name', 'dq']].to_dict(orient='records'), key=lambda x: x['dq'] if x['dq'] is not None else float('inf')),
            "ecom_dq": sorted(filtered_df[['project_id', 'brand_name', 'section_name', 'ecom_dq']].to_dict(orient='records'), key=lambda x: x['ecom_dq'] if x['ecom_dq'] is not None else float('inf')),
            "social_dq": sorted(filtered_df[['project_id', 'brand_name', 'section_name', 'social_dq']].to_dict(orient='records'), key=lambda x: x['social_dq'] if x['social_dq'] is not None else float('inf')),
            "paid_dq": sorted(filtered_df[['project_id', 'brand_name', 'section_name', 'paid_dq']].to_dict(orient='records'), key=lambda x: x['paid_dq'] if x['paid_dq'] is not None else float('inf')),
            "brand_perf_dq": sorted(filtered_df[['project_id', 'brand_name', 'section_name', 'brand_perf_dq']].to_dict(orient='records'), key=lambda x: x['brand_perf_dq'] if x['brand_perf_dq'] is not None else float('inf')),
            "statistics": statistics  # Adding the computed statistics to the response
        }

        # Convert NaN, Inf, and -Inf values in statistics to None
        for section in result['statistics'].values():
            for key, value in section.items():
                if value == float('inf') or value == float('-inf') or pd.isna(value):
                    section[key] = None

        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Define the endpoint to fetch filtered data
@app.post("/dq_filter_data")
async def filter_data(query: BrandQuery):
    result = fetch_filtered_data(query.project_id, query.brand_name)
    if not result:
        raise HTTPException(status_code=404, detail="No data found matching the criteria.")
    return result













# Pydantic model for the brand information
class BrandInfo(BaseModel):
    brand: str
    category: str
    sub_category: str
    

# Pydantic model for the request payload (project_id and brand_name)
class ProjectRequest(BaseModel):
    project_id: int
    brand_name: str

# Pydantic model for the response including competitors
class BrandResponse(BaseModel):
    project_id: int
    main_brand: BrandInfo
    competitors: list[BrandInfo]

def get_db_connection():
    """Create a new database connection."""
    return psycopg2.connect(**DB_PARAMS)

@app.post("/brands/competitors", response_model=BrandResponse)
async def get_competitors_for_project(payload: ProjectRequest):
    """Retrieve relevant competitor brands for a given project_id and brand_name."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Find the brand for the given project_id and brand_name in the public.normalisedvalue table
        query_project_brand = sql.SQL("""
            SELECT brandname 
            FROM public.normalisedvalue 
            WHERE project_id = %s AND brandname = %s
        """)
        
        cursor.execute(query_project_brand, (payload.project_id, payload.brand_name))
        project_brand_result = cursor.fetchone()
        
        if not project_brand_result:
            raise HTTPException(status_code=404, detail="Brand not found for the given project_id and brand_name")
        
        brand_name = project_brand_result[0]
        
        # 2. Retrieve the category and sub-category for the brand
        query_brand_info = sql.SQL("""
            SELECT category, sub_category 
            FROM dq.master_table_category_brand 
            WHERE brand = %s
        """)
        
        cursor.execute(query_brand_info, (brand_name,))
        brand_result = cursor.fetchone()
        
        if not brand_result:
            raise HTTPException(status_code=404, detail="Brand info not found in master table")
        
        category, sub_category = brand_result
        
        # 3. Find all competitors in the same category, excluding the main brand,
        # and ensure they exist in public.normalisedvalue for the given project_id
        query_competitors = sql.SQL("""
            SELECT brand, category, sub_category 
            FROM dq.master_table_category_brand AS mcc
            WHERE mcc.category = %s 
            AND mcc.brand != %s
            AND EXISTS (
                SELECT 1 
                FROM public.normalisedvalue AS nv
                WHERE nv.project_id = %s 
                AND nv.brandname = mcc.brand
            )
        """)
        
        cursor.execute(query_competitors, (category, brand_name, payload.project_id))
        competitor_brands = cursor.fetchall()
        
        # Construct the main brand info
        main_brand_info = BrandInfo(brand=brand_name, category=category, sub_category=sub_category)
        
        # Construct the competitors list
        competitors = [BrandInfo(brand=row[0], category=row[1], sub_category=row[2]) for row in competitor_brands]
        
        return BrandResponse(project_id=payload.project_id,main_brand=main_brand_info, competitors=competitors)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cursor.close()
        conn.close()










# class BrandCategoryInfo(BaseModel):
#     brand: str
#     category: str
#     organisation: str

# # Pydantic model for the request payload (project_id)
# class ProjectRequest(BaseModel):
#     project_id: int

# # Pydantic model for the response including the list of brands and categories
# class ProjectBrandsResponse(BaseModel):
#     project_id: int
#     brands_and_categories: list[BrandCategoryInfo]

# @app.post("/brands_categories_project_id", response_model=ProjectBrandsResponse)
# async def get_brands_and_categories_for_project(payload: ProjectRequest):
#     """Retrieve all brands and categories for the given project_id."""
#     try:
#         conn = get_db_connection()
#         cursor = conn.cursor()

#         # 1. Query to find all brands and categories associated with the given project_id
#         query = sql.SQL("""
#             SELECT DISTINCT nv.brandname, mcc.category, mcc.organisation
#             FROM public.normalisedvalue AS nv
#             JOIN dq.master_table_category_brand AS mcc
#             ON nv.brandname = mcc.brand
#             WHERE nv.project_id = %s
#         """)

#         cursor.execute(query, (payload.project_id,))
#         brands_and_categories = cursor.fetchall()

#         if not brands_and_categories:
#             raise HTTPException(status_code=404, detail="No brands found for the given project_id")

#         # Construct the response
#         brands_and_categories_info = [BrandCategoryInfo(brand=row[0], category=row[1],organisation=row[2]) for row in brands_and_categories]

#         return ProjectBrandsResponse(project_id=payload.project_id, brands_and_categories=brands_and_categories_info)

#     except Exception as e:
#         raise HTTPException(status_code=500, detail=str(e))
    
#     finally:
#         cursor.close()
#         conn.close()


@app.get("/brands/{brand_name}/project_id/{project_id}", response_model=BrandResponse)
async def get_competitors_for_project_get(brand_name: str, project_id: int):
    """Retrieve relevant competitor brands for a given project_id and brand_name using GET method."""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        # 1. Find the brand for the given project_id and brand_name in the public.normalisedvalue table
        query_project_brand = sql.SQL("""
            SELECT brandname 
            FROM public.normalisedvalue 
            WHERE project_id = %s AND brandname = %s
        """)
        
        cursor.execute(query_project_brand, (project_id, brand_name))
        project_brand_result = cursor.fetchone()
        
        if not project_brand_result:
            raise HTTPException(status_code=404, detail="Brand not found for the given project_id and brand_name")
        
        brand_name = project_brand_result[0]
        
        # 2. Retrieve the category and sub-category for the brand
        query_brand_info = sql.SQL("""
            SELECT category, sub_category 
            FROM dq.master_table_category_brand 
            WHERE brand = %s
        """)
        
        cursor.execute(query_brand_info, (brand_name,))
        brand_result = cursor.fetchone()
        
        if not brand_result:
            raise HTTPException(status_code=404, detail="Brand info not found in master table")
        
        category, sub_category = brand_result
        
        # 3. Find all competitors in the same category, excluding the main brand,
        # and ensure they exist in public.normalisedvalue for the given project_id
        query_competitors = sql.SQL("""
            SELECT brand, category, sub_category 
            FROM dq.master_table_category_brand AS mcc
            WHERE mcc.category = %s 
            AND mcc.brand != %s
            AND EXISTS (
                SELECT 1 
                FROM public.normalisedvalue AS nv
                WHERE nv.project_id = %s 
                AND nv.brandname = mcc.brand
            )
        """)
        
        cursor.execute(query_competitors, (category, brand_name, project_id))
        competitor_brands = cursor.fetchall()
        
        # Construct the main brand info
        main_brand_info = BrandInfo(brand=brand_name, category=category, sub_category=sub_category)
        
        # Construct the competitors list
        competitors = [BrandInfo(brand=row[0], category=row[1], sub_category=row[2]) for row in competitor_brands]
        
        return BrandResponse(project_id=project_id, main_brand=main_brand_info, competitors=competitors)

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
    finally:
        cursor.close()
        conn.close()























