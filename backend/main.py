from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os
from datetime import datetime

app = FastAPI()

static_dir = os.path.join(os.path.dirname(__file__), "static")
os.makedirs(static_dir, exist_ok=True)

# 1. API routes first (they take priority)
@app.get("/api/health")
async def health():
    try:
        # Test database connection
        from backend_functions.database_functions import get_conn
        conn = get_conn()
        conn.close()
        db_status = "ok"
    except Exception as e:
        db_status = f"error: {str(e)}"

    return {
        "status": "ok",
        "database": db_status,
        "timestamp": datetime.now().isoformat()
    }

# 2. Serve Next.js static assets from /_next
next_static_dir = os.path.join(static_dir, "_next")
if os.path.exists(next_static_dir):
    app.mount("/_next", StaticFiles(directory=next_static_dir), name="_next")

# 3. SPA fallback – serve index.html for any other route
@app.get("/{path:path}")
async def catch_all(path: str):
    index_path = os.path.join(static_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"error": "Not found"}