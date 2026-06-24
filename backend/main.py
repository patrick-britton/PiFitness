from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI()

# Ensure static directory exists (create if missing)
static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)

# Mount static files (will serve index.html and other assets)
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")

# Health check endpoint
@app.get("/api/health")
async def health():
    return {"status": "ok"}

# SPA fallback – return index.html for any non‑API route if it exists
@app.get("/{path:path}")
async def catch_all(path: str):
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"error": "Not found"}