from fastapi import Request
from starlette.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
import requests
import os

class BearerTokenMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        # Skip authentication for the health check endpoint
        if request.url.path == "/health":
            return await call_next(request)
        
        # Check if the Authorization header is present
        if "Authorization" not in request.headers:
            return JSONResponse(status_code=401, content={"errorCode": 401, "statusCode": "UNAUTHORIZED", "message": "Token Missing"})
        
        # Extract the token from the Authorization header
        token_header = request.headers["Authorization"]
        
        # Check if the token starts with "Bearer "
        if not token_header.startswith("Bearer "):
            return JSONResponse(status_code=401, content={"errorCode": 401, "statusCode": "UNAUTHORIZED", "message": "Token Missing"})
        
        # Extract the actual token
        token = token_header.split("Bearer ")[-1].strip()
        
        # Verify the token with the centralized authentication server
        try:
            response = requests.post(
                os.getenv("ONBOARDING_PORTAL_SERVER_URL") + '/auth/isLoggedIn',
                headers={"Authorization": f"Bearer {token}"},
                json={
                    "toolName": "DIGI-CADENCE"
                }
            )

            if response.status_code != 200:
                error_data = response.json()
                return JSONResponse(status_code=response.status_code, content={
                    "errorCode": error_data.get("errorCode", 401),
                    "statusCode": error_data.get("statusCode", "UNAUTHORIZED"),
                    "message": error_data.get("message", "Unauthorized")
                })

            # If the token is valid, proceed with the request
            response = await call_next(request)
            return response

        except requests.exceptions.RequestException as e:
            return JSONResponse(status_code=500, content={
                "errorCode": 500,
                "statusCode": "INTERNAL_SERVER_ERROR",
                "message": str(e)
            })
