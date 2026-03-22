# BeeTwin – Beehive Digital Twin Monitoring System

BeeTwin is a real-time digital twin system for monitoring beehive conditions.  
It combines sensor data streaming, backend state estimation using Kalman filtering, and a live dashboard to visualize hive health.

The system contains three main components:

- **Backend** – FastAPI server that processes measurements and runs the Kalman filter.
- **Frontend** – React dashboard that visualizes hive conditions in real time.
- **Simulator** – Streams hive sensor data to simulate real beehive sensors.

---

# Project Structure

```
beetwin-prototype
│
├ backend
│   ├ app
│   ├ data
│   └ venv
│
├ frontend
├ notebooks
│
├ simulator.py
├ requirements.txt
├ run_all.bat
└ README.md
```

---

# How to Run the System (Windows)

The easiest way to run the entire system is using the provided launcher.

## Step 1 — Open the project folder

Open the project root directory:

```
beetwin-prototype
```

You should see:

```
backend
frontend
notebooks
simulator.py
run_all.bat
README.md
```

---

## Step 2 — Run the launcher

Double-click:

```
run_all.bat
```

Or run it from PowerShell:

```powershell
.\run_all.bat
```

---

## Step 3 — Wait for backend initialization

The backend loads historical hive data during startup.  
This may take **approximately 45–50 seconds**.

You will see messages such as:

```
[preload] Inserted 10000 records...
[preload] Inserted 20000 records...
Application startup complete.
```

Once this finishes, the system is ready.

---

## Step 4 — Open the dashboard

Open  browser and go to:

```
http://localhost:5173
```

Backend API documentation is available at:

```
http://127.0.0.1:8000/docs
```

---

# What the Launcher Starts

The `run_all.bat` script automatically starts:

1. **Backend API (FastAPI)**
2. **Frontend Dashboard (React)**
3. **Hive Sensor Simulator**

Each component opens in its own terminal window.

The simulator continuously streams hive measurements to the backend to demonstrate real-time monitoring and Kalman filter state updates.



# Manual Run (Optional)


## Start Backend

```bash
cd backend
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --reload --port 8000
```

---

## Start Frontend

Open a new terminal:

```bash
cd frontend
npm install
npm run dev
```

---

## Start Simulator

Open another terminal:

```bash
cd backend
python simulator.py
```

---

# System Architecture

```
Hive Sensor Simulator
        ↓
Backend API (FastAPI)
        ↓
Kalman Filter State Estimation
        ↓
Database / State Store
        ↓
Frontend Dashboard
```

The backend fuses sensor data using a Kalman filter to handle noisy measurements and missing values, producing stable real-time hive state estimates.

---

# Key Features

- Real-time hive condition monitoring
- Kalman filter based sensor fusion
- Handling of noisy and missing sensor data
- Live dashboard visualization
- Historical data initialization for realistic digital twin behavior