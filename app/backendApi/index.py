from fastapi import FastAPI
from app.backendApi.routes.Brand_image import app as brand_image
from app.backendApi.auth_middleware import BearerTokenMiddleware
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

# app.add_middleware(BearerTokenMiddleware)

app.include_router(brand_image)