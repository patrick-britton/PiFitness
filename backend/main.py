"""
PiFitness FastAPI Application
=============================

Main entry point for the PiFitness API backend.
Serves API endpoints and optionally serves the compiled React frontend
as static assets in production.

Endpoints:
    GET /api/health - Health check (database connectivity)
    GET /api/activities - List activities
    GET /api/activities/{id} - Get activity details
    GET /api/health/weight-targets - Get weight targets
    GET /api/health/heartrate - Get heart rate data
    GET /api/admin/tasks - List tasks
"""

import os
from datetime import datetime

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.exceptions import RequestValidationError
from starlette.exceptions import HTTPException as StarletteHTTPException

from backend.config import get_settings
from backend.api import activities, health, admin, music

# ---------------------------------------------------------------------------
# Application Initialization
# ---------------------------------------------------------------------------

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="1.0.0",
    debug=settings.debug,
)

# ---------------------------------------------------------------------------
# CORS Middleware
# ---------------------------------------------------------------------------

app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ---------------------------------------------------------------------------
# Global Error Handlers
# ---------------------------------------------------------------------------


@app.exception_handler(StarletteHTTPException)
async def http_exception_handler(request: Request, exc: StarletteHTTPException):
    """Return structured JSON for HTTP errors instead of HTML."""
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "detail": exc.detail,
            "status": exc.status_code,
            "type": "HTTPException",
        },
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(
    request: Request, exc: RequestValidationError
):
    """Return structured JSON for Pydantic validation errors."""
    return JSONResponse(
        status_code=422,
        content={
            "detail": exc.errors(),
            "body": exc.body,
            "status": 422,
            "type": "ValidationError",
        },
    )


@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Catch-all handler for unhandled exceptions."""
    return JSONResponse(
        status_code=500,
        content={
            "detail": "Internal server error",
            "status": 500,
            "type": "InternalServerError",
        },
    )

# ---------------------------------------------------------------------------
# API Routes
# ---------------------------------------------------------------------------

# Include API routers
app.include_router(activities.router)
app.include_router(health.router)
app.include_router(admin.router)
app.include_router(music.router)

@app.get("/api/health")
async def health():
    """Health check endpoint that tests database connectivity."""
    try:
        from backend_functions.database_functions import get_conn

        conn = get_conn()
        conn.close()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "ok",
        "database": db_status,
        "timestamp": datetime.now().isoformat(),
    }

# ---------------------------------------------------------------------------
# Static File Serving (Production) - DISABLED
# ---------------------------------------------------------------------------
# Note: Static file serving is now handled by Next.js server directly.
# The nginx configuration routes /api to FastAPI and all other traffic to Next.js.

# static_dir = os.path.join(os.path.dirname(__file__), "static")
# os.makedirs(static_dir, exist_ok=True)

# Serve Next.js static assets from /_next (built by `npm run build`)
# next_static_dir = os.path.join(static_dir, "_next")
# if os.path.exists(next_static_dir):
#     app.mount("/_next", StaticFiles(directory=next_static_dir), name="_next")

# SPA fallback — serve index.html for any other route
# @app.get("/{path:path}")
# async def catch_all(path: str):
#     index_path = os.path.join(static_dir, "index.html")
#     if os.path.exists(index_path):
#         return FileResponse(index_path)
#     return {"error": "Not found"}
