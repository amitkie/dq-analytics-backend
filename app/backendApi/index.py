from fastapi import FastAPI, HTTPException, Request
from app.backendApi.middlewares.auth_middleware import BearerTokenMiddleware
from fastapi.middleware.cors import CORSMiddleware
import logging
from datetime import datetime
from fastapi.responses import JSONResponse
import psycopg2
from psycopg2 import sql

from app.backendApi.config.db import DB_PARAMS

from app.backendApi.routes.analytics_kpi_section_updated import app as analytics_routes
from app.backendApi.routes.brand_sub_image_defination import app as brand_sub_image_routes
from app.backendApi.routes.category_wise import app as category_routes
from app.backendApi.routes.dq_norm_combined import app as dq_norm_routes
from app.backendApi.routes.get_all_brand_data import app as brand_routes
from app.backendApi.routes.m_p_s_report_norm_brand_weight_combined import app as m_p_s_routes
from app.backendApi.routes.metric_health_combined import app as metric_health_routes
from app.backendApi.routes.multi import app as multi_routes
from app.backendApi.routes.normalisation_api import app as normalisation_routes
from app.backendApi.routes.supertheme_combined import app as supertheme_routes
from app.backendApi.routes.user_details_greeting import app as user_details_greeting_routes

app = FastAPI()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add CORS middleware
origins = [
    "http://localhost",
    "http://localhost:3000",
    "https://example.com",
    "https://m594bmgj-7033.inc1.devtunnels.ms",
    "https://*.devtunnels.ms",
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

app.include_router(analytics_routes)
app.include_router(brand_sub_image_routes)
app.include_router(category_routes)
app.include_router(dq_norm_routes)
app.include_router(brand_routes)
app.include_router(m_p_s_routes)
app.include_router(metric_health_routes)
app.include_router(multi_routes)
app.include_router(normalisation_routes)
app.include_router(supertheme_routes)
app.include_router(user_details_greeting_routes)

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint to verify service status"""
    try:
        # Test database connection
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1"))
            cursor.fetchone()
        return {
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "database": "connected"
        }
    except Exception as e:
        raise HTTPException(
            status_code=503,
            detail=f"Service unhealthy: {str(e)}"
        )
    finally:
        if conn:
            conn.close()

# Additional error handling middleware
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler for unhandled errors"""
    error_message = str(exc)
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "detail": error_message,
            "path": request.url.path
        }
    )

# Startup and shutdown events
@app.on_event("startup")
async def startup_event():
    """Execute startup tasks"""
    logger.info("Starting up the application...")
    try:
        # Test database connection on startup
        conn = psycopg2.connect(**DB_PARAMS)
        with conn.cursor() as cursor:
            cursor.execute(sql.SQL("SELECT 1"))
            cursor.fetchone()
        logger.info("Database connection successful")
    except Exception as e:
        logger.error(f"Error connecting to database: {e}")
        raise e
    finally:
        if conn:
            conn.close()

@app.on_event("shutdown")
async def shutdown_event():
    """Execute shutdown tasks"""
    logger.info("Shutting down the application...")
