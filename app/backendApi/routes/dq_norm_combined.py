from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
import numpy as np
import psycopg2
import json
from typing import List, Dict, Any
from app.backendApi.config.db import DB_PARAMS

app = APIRouter()

# Create routers
router_metrics = APIRouter(prefix="/metrics")
router_brand = APIRouter(prefix="/brand")
router_normalized = APIRouter(prefix="/normalized")

SCHEMA_NAME = 'public'

# Pydantic models
class MetricsRequestPayload(BaseModel):
    project_id: int

class BrandRequestPayload(BaseModel):
    brand_name: str
    project_ids: List[int]

class NormalizedRequestPayload(BaseModel):
    project_ids: List[int]

# Database connection function
def connect_to_db():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        raise HTTPException(status_code=500, detail="Unable to connect to the database")

# Metrics Router Functions
def join_table(conn) -> pd.DataFrame:
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name
    FROM {SCHEMA_NAME}.normalisedvalue nv
    LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def load_master_table_metrics(conn, project_id: int) -> pd.DataFrame:
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.normalisedvalue
    WHERE project_id = {project_id}
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def calculate_final_score(group: pd.DataFrame) -> float:
    weight_sum = group['weights'].sum()
    if weight_sum == 0:
        return np.nan
    score = (group['weights'] * group['normalized']).sum() / weight_sum
    return score

def compute_final_scores_metrics(df: pd.DataFrame) -> pd.DataFrame:
    overall_results = pd.DataFrame()
    additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

    if not df.empty:
        sum_of_product = df.groupby(['brandname', 'categoryname']).apply(calculate_final_score)
        
        overall_results = pd.DataFrame({
            'Brand_Name': sum_of_product.index.get_level_values('brandname'),
            'Category_Name': sum_of_product.index.get_level_values('categoryname'),
            'Overall_Final_Score': sum_of_product.values
        })

        sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values

        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        overall_results['id'] = range(1, len(overall_results) + 1)

    final_result = overall_results[['Brand_Name', 'Overall_Final_Score', 'Category_Name'] + additional_columns]
    return final_result

@router_metrics.post("/get_data")
async def get_metrics_data(payload: MetricsRequestPayload):
    conn = connect_to_db()
    try:
        df = join_table(conn)
        df = load_master_table_metrics(conn, payload.project_id)
        if df is not None and not df.empty:
            final_scores_df = compute_final_scores_metrics(df)
            result = final_scores_df.to_dict(orient='records')
            return {"data": result}
        else:
            return {"data": []}
    finally:
        conn.close()

# Brand Router Functions
def get_category_name(conn, brand_name: str) -> str:
    query = f"""
    SELECT c.name
    FROM {SCHEMA_NAME}.brands b
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    WHERE b.name = %s
    """
    try:
        result = pd.read_sql_query(query, conn, params=(brand_name,))
        if result.empty:
            raise HTTPException(status_code=404, detail="Brand not found")
        return result.iloc[0]["name"]
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the category name: {e}")

def load_master_table_brand(conn, brand_name: str, project_ids: List[int]) -> pd.DataFrame:
    category_name = get_category_name(conn, brand_name)
    
    query = f"""
    SELECT nv.*, b.name AS brand_name, c.name AS category_name
    FROM {SCHEMA_NAME}.normalisedvalue nv
    LEFT JOIN {SCHEMA_NAME}.brands b ON nv.brandName = b.name
    LEFT JOIN {SCHEMA_NAME}.categories c ON b.category_id = c.id
    WHERE (b.name = %s OR c.name = %s) AND nv.project_id IN %s
    """
    try:
        df = pd.read_sql_query(query, conn, params=(brand_name, category_name, tuple(project_ids)))
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def compute_final_scores_brand(df: pd.DataFrame) -> pd.DataFrame:
    overall_results = pd.DataFrame()
    additional_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']

    if not df.empty:
        sum_of_product = df.groupby('brandname').apply(calculate_final_score)
        overall_results = pd.DataFrame({
            'Brand_Name': sum_of_product.index,
            'Overall_Final_Score': sum_of_product.values
        })

        sectionwise = df.groupby(['brandname', 'sectionname']).apply(calculate_final_score).unstack(fill_value=0)
        for section in sectionwise.columns:
            overall_results[section] = sectionwise[section].values

        for col in additional_columns:
            if col not in overall_results.columns:
                overall_results[col] = 0

        overall_results['id'] = range(1, len(overall_results) + 1)

    final_result = overall_results[['Brand_Name', 'Overall_Final_Score'] + additional_columns]
    return final_result

def compute_statistical_metrics(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {}
    
    score_stats = {
        '50th_percentile': df['Overall_Final_Score'].quantile(0.50),
        '75th_percentile': df['Overall_Final_Score'].quantile(0.75),
    }
    
    section_columns = ['Marketplace', 'Socialwatch', 'Digital Spends', 'Organic Performance']
    section_stats = {}
    
    for section in section_columns:
        if section in df.columns:
            section_stats[section] = {
                '50th_percentile': df[section].quantile(0.50),
                '75th_percentile': df[section].quantile(0.75)
            }
    
    return {
        'overall_score_stats': score_stats,
        'section_stats': section_stats
    }

@router_brand.post("/get_brand_data")
async def get_brand_data(payload: BrandRequestPayload):
    conn = connect_to_db()
    try:
        df = load_master_table_brand(conn, payload.brand_name, payload.project_ids)
        
        response_data = {
            "data": [],
            "statistics": {}
        }
        
        if df is not None and not df.empty:
            final_scores_df = compute_final_scores_brand(df)
            statistical_metrics = compute_statistical_metrics(final_scores_df)
            
            response_data["information"] = final_scores_df.to_dict(orient="records")
            response_data["statistics"] = statistical_metrics
            
            brand_data = final_scores_df[final_scores_df['Brand_Name'] == payload.brand_name]
            
            if not brand_data.empty:
                response_data["data"] = brand_data.to_dict(orient="records")
        
        return response_data
    finally:
        conn.close()

# Normalized Router Functions
def create_normalised_value_table(conn):
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {SCHEMA_NAME}.normalisedvalue (
        project_id INTEGER,
        categoryId INTEGER,
        sectionId INTEGER,
        metricId INTEGER,
        platformId INTEGER,
        weights FLOAT,
        benchmarkValue INTEGER,
        actualValue INTEGER,
        normalized INTEGER,
        brandName VARCHAR(255),
        sectionName VARCHAR(255),
        categoryName VARCHAR(255),
        metricname VARCHAR(255),
        platformname VARCHAR(255),
        type VARCHAR(50),
        UNIQUE (
            project_id, categoryId, sectionId, metricId, platformId, 
            weights, benchmarkValue, actualValue, normalized, brandName, 
            sectionName, categoryName, metricname, platformname, type
        )
    )
    """
    with conn.cursor() as cursor:
        try:
            cursor.execute(create_table_query)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f"Error creating table: {e}")

def insert_normalised_value(conn, df):
    insert_query = f"""
    INSERT INTO {SCHEMA_NAME}.normalisedvalue 
    (project_id, categoryId, sectionId, metricId, platformId, weights, benchmarkValue, 
     actualValue, normalized, brandName, sectionName, categoryName, metricname, platformname, type)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (project_id, categoryId, sectionId, metricId, platformId, 
                weights, benchmarkValue, actualValue, normalized, brandName, 
                sectionName, categoryName, metricname, platformname, type)
    DO NOTHING
    """
    
    with conn.cursor() as cursor:
        try:
            for _, row in df.iterrows():
                row_values = (
                    int(row['project_id']), 
                    int(row['categoryid']), 
                    int(row['sectionid']), 
                    int(row['metricid']), 
                    int(row['platformid']), 
                    float(row['weights']), 
                    int(row['benchmarkValue']), 
                    int(row['actualValue']), 
                    int(row['normalized']),
                    str(row['brandName']),
                    str(row['sectionName']),
                    str(row['categoryName']),
                    str(row['metricname']),
                    str(row['platformname']),
                    str(row['type'])
                )
                cursor.execute(insert_query, row_values)
            conn.commit()
        except psycopg2.Error as e:
            conn.rollback()
            raise HTTPException(status_code=500, detail=f'Error inserting data: {e}')

def load_master_table_normalized(conn, project_ids: List[int]):
    ids_str = ','.join(map(str, project_ids))
    query = f"""
    SELECT * FROM {SCHEMA_NAME}.project_benchmarks
    WHERE project_id IN ({ids_str})
    """
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Error querying the benchmark table: {e}")

def parse_benchmark(df):
    output_data = []

    for _, row in df.iterrows():
        try:
            benchmarks = json.loads(row["benchmarks"])
            for benchmark in benchmarks:
                category_id = benchmark["categoryId"]
                category_name = benchmark["categoryName"]
                percentile = benchmark.get("percentile", np.nan)
                benchmark_value = benchmark.get("benchmarkValue", benchmark.get("overallValue", np.nan))
                section_name = benchmark["sectionName"]
                actual_value_dict = benchmark.get("actualValue", {})

                if isinstance(actual_value_dict, dict) and actual_value_dict:
                    for brand_name, actual_value in actual_value_dict.items():
                        if actual_value is None or actual_value == "":
                            continue
                        
                        type_value = 'category' if row["isCategory"] == True else 'overall' if row["isOverall"] == True else 'unknown'

                        output_row = {
                            "project_id": row["project_id"],
                            "categoryid": category_id,
                            "sectionid": row["sectionId"],
                            "metricid": row["metricId"],
                            "platformid": row["platformId"],
                            "weights": row["weights"],
                            "percentile": percentile,
                            "benchmarkValue": benchmark_value,
                            "actualValue": actual_value,
                            "normalized": None,
                            "brandName": brand_name,
                            "sectionName": section_name,
                            "categoryName": category_name,
                            'metricName': section_name,
                            "platformName": section_name,
                            "type": type_value
                        }
                        if output_row not in output_data:
                            output_data.append(output_row)
                            
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"Error decoding JSON: {e}")

    df_output = pd.DataFrame(output_data)
    df_output = df_output.drop_duplicates()
    return df_output

def normalised_value(df):
    df['direct'] = np.nan
    df['indirect'] = np.nan

    direct_condition = df['actualValue'] < df['benchmarkValue']
    max_actual_value = df["actualValue"].max()
    
    df.loc[direct_condition, 'direct'] = df['actualValue'] / df['benchmarkValue'] * 90
    df.loc[df['actualValue'] >= df['benchmarkValue'], 'direct'] = df['actualValue'].apply(
        lambda x: 100 if x == max_actual_value else 90)
    df.loc[df['weights'] == 0, 'direct'] = 0

    indirect_condition = df['actualValue'] >= df['benchmarkValue']
    min_actual_value = df["actualValue"].min()

    df.loc[indirect_condition, 'indirect'] = df['benchmarkValue'] / df['actualValue'] * 90
    df.loc[~indirect_condition, 'indirect'] = df['actualValue'].apply(
        lambda y: 100 if y == min_actual_value else 90)
    df.loc[df['weights'] == 0, 'indirect'] = 0

    df['normalized'] = np.where(df['percentile'] == 0.75, df['direct'], df['indirect'])
    
    df.drop(columns=['direct', 'indirect'], inplace=True)
    df['normalized'] = np.floor(df['normalized']/10)*10
    df['normalized'] = df['normalized'].replace([np.inf, -np.inf], np.nan).fillna(0)

    return df

@router_normalized.post("/normalized_value")
async def normalize_benchmarks(payload: NormalizedRequestPayload):
    conn = connect_to_db()
    
    try:
        df = load_master_table_normalized(conn, payload.project_ids)
        if df is None or df.empty:
            raise HTTPException(status_code=404, detail="No data found for the given project_ids")
        
        df_output = parse_benchmark(df)
        if not all(col in df_output.columns for col in ['actualValue','benchmarkValue','percentile']):
            return {"message": "Skipping normalization; required values are missing."}
        
        df_output = normalised_value(df_output)

        # Get reference data
        brands_query = f"SELECT name as brand_name, id as brand_ids, category_id FROM {SCHEMA_NAME}.brands"
        categories_query = f"SELECT name as category_name,id FROM {SCHEMA_NAME}.categories"
        metrics_query = f"SELECT name as metricname,id FROM {SCHEMA_NAME}.metrics"
        platforms_query = f"SELECT name as platformname, id FROM {SCHEMA_NAME}.platforms"
     
        brands_df = pd.read_sql_query(brands_query, conn)
        categories_df = pd.read_sql_query(categories_query, conn)
        metrics_df = pd.read_sql_query(metrics_query, conn)
        platforms_df = pd.read_sql_query(platforms_query, conn)

        # Join all data
        joined_df = df_output.merge(brands_df, left_on='brandName', right_on='brand_name', how='left', suffixes=('', '_brand'))
        joined_df_cat = joined_df.merge(categories_df, left_on='category_id', right_on='id', how='left', suffixes=('', '_category'))
        joined_df_plat = joined_df_cat.merge(platforms_df, left_on='platformid', right_on='id', how='left', suffixes=('', '_platform'))
        joined_df = joined_df_plat.merge(metrics_df, left_on='metricid', right_on='id', how='left', suffixes=('', '_metric'))

        # Clean up columns
        columns_to_drop = ['categoryid','categoryname','brandName','platformName','metricName','id_platform','id_metric','id','categoryName']
        joined_df = joined_df.drop(columns=columns_to_drop, errors='ignore')

        joined_df = joined_df.rename(columns={
            'benchmarkvalue': 'benchmarkValue',
            'actualvalue': 'actualValue',
            'brand_name': 'brandName',
            'sectionname': 'sectionName',
            'category_name': 'categoryName',
            'category_id': 'categoryid',
            'platformName': 'platformName',
            'metricName':'metricName'
        })

        joined_df = joined_df.drop_duplicates()
        
        # Create and update normalized value table
        create_normalised_value_table(conn)
        insert_normalised_value(conn, joined_df)

        joined_df = joined_df.replace([np.inf, -np.inf], np.nan).fillna(0)
        
        return joined_df.to_dict(orient='records')
    
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database error: {e}")
    
    finally:
        conn.close()

# Include all routers
app.include_router(router_metrics)
app.include_router(router_brand)
app.include_router(router_normalized)

# Root endpoint for health check
@app.get("/")
async def root():
    return {"message": "API is running"}

# Documentation endpoints
@app.get("/docs")
async def get_documentation():
    return {
        "endpoints": {
            "/metrics/get_data": "Get metrics data for a specific project",
            "/brand/get_brand_data": "Get brand-specific data with statistics",
            "/normalized/normalized_value": "Get normalized benchmark values"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)