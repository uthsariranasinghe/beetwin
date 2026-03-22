#!/usr/bin/env bash

set -e

echo "Starting Beehive Digital Twin development environment..."

PROJECT_ROOT=$(pwd)

echo "--------------------------------------"
echo "1) Starting backend (FastAPI)"
echo "--------------------------------------"

cd backend

# activate virtual environment if exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

uvicorn app.main:app --reload --host 0.0.0.0 --port 8000 &

BACKEND_PID=$!

cd "$PROJECT_ROOT"


echo " Starting frontend (React/Vite)"


cd frontend

# install packages if node_modules missing
if [ ! -d "node_modules" ]; then
    npm install
fi

npm run dev &

FRONTEND_PID=$!

cd "$PROJECT_ROOT"

echo " Starting simulator"


# activate backend environment for simulator
cd backend

if [ -d "venv" ]; then
    source venv/bin/activate
fi

cd "$PROJECT_ROOT"

python simulator.py &

SIMULATOR_PID=$!

echo "--------------------------------------"
echo "System started"
echo ""
echo "Backend:   http://localhost:8000"
echo "Frontend:  http://localhost:5173"
echo "API docs:  http://localhost:8000/docs"
echo ""
echo "Press Ctrl+C to stop everything"
echo "--------------------------------------"

wait $BACKEND_PID $FRONTEND_PID $SIMULATOR_PID