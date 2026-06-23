#!/bin/bash

# ===============================
# PiFitness Deployment Script (Nginx)
# ===============================

PROJECT_DIR=~/PiFitness
VENV_DIR=$PROJECT_DIR/venv
STREAMLIT_PORT=8501
REQ_FILE="$PROJECT_DIR/deployment/library_requirements.txt"

# -------------------------------
# 1. Activate virtual environment
# -------------------------------
if [ -d "$VENV_DIR" ]; then
    source $VENV_DIR/bin/activate
else
    echo "Virtual environment not found at $VENV_DIR. Creating..."
    python3 -m venv $VENV_DIR
    source $VENV_DIR/bin/activate
    pip install --upgrade pip
    if [ -f "$REQ_FILE" ]; then
        pip install -r "$REQ_FILE"
    fi
fi

# -------------------------------
# 2. Stop Streamlit if running
# -------------------------------
STREAMLIT_PID=$(pgrep -f "streamlit run $PROJECT_DIR/pi_fitness.py")
if [ -n "$STREAMLIT_PID" ]; then
    echo "Stopping existing Streamlit process (PID $STREAMLIT_PID)..."
    kill -9 $STREAMLIT_PID
else
    echo "No running Streamlit process found."
fi

# -------------------------------
# 3. Update code from GitHub
# -------------------------------
echo "Updating project from GitHub..."
cd $PROJECT_DIR
git fetch origin
git reset --hard origin/streamlit-prd
git clean -fd

# -------------------------------
# 4. Install/update Python packages
# -------------------------------
if [ -f "$REQ_FILE" ]; then
    echo "Installing/updating Python dependencies from $REQ_FILE..."
    pip install --upgrade pip
    pip install -r "$REQ_FILE"
else
    echo "library_requirements.txt not found at $REQ_FILE!"
fi

# -------------------------------
# 5. Start Streamlit if not running
# -------------------------------
STREAMLIT_PID=$(pgrep -f "streamlit run $PROJECT_DIR/pi_fitness.py")
if [ -z "$STREAMLIT_PID" ]; then
    echo "Starting Streamlit on port $STREAMLIT_PORT..."
    nohup streamlit run $PROJECT_DIR/pi_fitness.py \
        --server.port $STREAMLIT_PORT \
        --server.headless true \
        --server.enableCORS false \
        --server.enableXsrfProtection false \
        > $PROJECT_DIR/streamlit.log 2>&1 &
else
    echo "Streamlit already running (PID $STREAMLIT_PID)."
fi

echo "Deployment complete! Streamlit logs: $PROJECT_DIR/streamlit.log"
