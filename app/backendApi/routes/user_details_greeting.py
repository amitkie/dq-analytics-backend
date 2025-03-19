from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
import pandas as pd
import psycopg2
from datetime import datetime
from app.backendApi.config.db import DB_PARAMS

# Schema names
SCHEMA_NAME = 'onboarding_portal'
TIME_SCHEMA_NAME = 'dq'

app = APIRouter()

# Request payload for accepting user ID
class RequestPayload(BaseModel):
    userid: int

# Function to connect to the PostgreSQL database
def connect_to_db():
    """Connect to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        return conn
    except psycopg2.Error as e:
        print(f"Unable to connect to the database: {e}")
        return None

# Function to execute a query and return a DataFrame
def execute_query(query: str):
    conn = connect_to_db()
    if not conn:
        raise HTTPException(status_code=500, detail="Unable to connect to the database")
    
    try:
        df = pd.read_sql_query(query, conn)
        return df
    except psycopg2.Error as e:
        raise HTTPException(status_code=500, detail=f"Database query error: {str(e)}")
    finally:
        conn.close()

# API 1: Fetch user details
@app.post("/user_details")
def get_user_details(payload: RequestPayload):
    query = f'SELECT first_name FROM {SCHEMA_NAME}."user_details" WHERE userid = {payload.userid}'
    df = execute_query(query)
    if df.empty:
        raise HTTPException(status_code=404, detail="User not found")
    
    user_name = df.iloc[0]['first_name']
    now = datetime.now()
    greeting = (
        "Good morning!" if 5 <= now.hour < 12 else
        "Good afternoon!" if 12 <= now.hour < 18 else
        "Good evening!" if 18 <= now.hour < 22 else
        "Good night!"
    )
    
    return {
        "user_id": payload.userid,
        "user_name": user_name,
        "date": now.strftime("%Y-%m-%d"),
        "time": now.strftime("%H:%M:%S"),
        "greeting": greeting
    }

# API 2: Fetch plan details
@app.post("/plan_details")
def get_plan_details(payload: RequestPayload):
    query = f"""
    SELECT ud.sub_userid, pd.userid, pd.first_name, pd.last_name, pd.subscription_date, pd.subscription_type, 
           pd.payment_status, pd.tools_subscribed, pd.ecom_platforms, pd.social_platforms, pd.categories, 
           pd.own_brands, pd.competition_brands, pd.own_skus, pd.competition_skus, pd.next_payment_date 
    FROM {SCHEMA_NAME}.plan_payment pd 
    LEFT JOIN {SCHEMA_NAME}.user_details ud ON pd.userid = ud.userid
    WHERE pd.userid = {payload.userid}
    """
    df = execute_query(query)
    if df.empty:
        raise HTTPException(status_code=404, detail="User data not found")
    return {"plan_details": df.to_dict(orient="records")}

# API 3: Get subscribed and non-subscribed tools
@app.post("/total_subscribed")
def get_subscribe_details(payload: RequestPayload):
    query = f'SELECT first_name, last_name, tools_subscribed FROM {SCHEMA_NAME}."plan_payment" WHERE userid = {payload.userid}'
    df = execute_query(query)
    if df.empty:
        raise HTTPException(status_code=404, detail="User not found")
    
    first_name = df.iloc[0]['first_name']
    last_name = df.iloc[0]['last_name']
    all_tools = {"META-360 & SOV", "Digi-Cadence", "Spendverse"}
    subscribed_tools = set(map(str.strip, df.iloc[0]['tools_subscribed'].split(",")))
    not_subscribed_tools = all_tools - subscribed_tools
    
    return {
        "first_Name": first_name,
        "Last_Name": last_name,
        "Tools_Subscribed": ", ".join(sorted(subscribed_tools)),
        "Tools_Not_Subscribed": ", ".join(sorted(not_subscribed_tools))
    }

# API 4: Fetch date range for tables with a date column
# API 4: Fetch overall min and max date
@app.get("/date_ranges")
def fetch_min_max_dates():
    query = f"""
    SELECT MIN(date) AS min_date, MAX(date) AS max_date 
    FROM (SELECT date FROM {TIME_SCHEMA_NAME}.amazon
          UNION ALL 
          SELECT date FROM {TIME_SCHEMA_NAME}.amazon___display_campaigns) AS combined;
    """
    df = execute_query(query)
    if df.empty:
        raise HTTPException(status_code=404, detail="No date data found")
    
    min_date = df.iloc[0]['min_date'].strftime("%m/%d/%Y")
    max_date = df.iloc[0]['max_date'].strftime("%m/%d/%Y")
    
    return {"Minimum Date": min_date, "Maximum Date": max_date}
