# 🏀 Real-Time NBA Game Insight Pipeline

A real-time machine learning pipeline that simulates, processes, models, and visualizes live NBA game data. This project demonstrates a full-stack MLOps architecture—from streaming ingestion to live dashboard prediction—using real NBA play-by-play data.

---

## 📌 Project Overview

This project is designed to demonstrate an end-to-end ML + Data Engineering pipeline using live (or simulated) NBA data. It features real-time data ingestion, streaming transformations, predictive modeling, and interactive visualization. The goal is to estimate dynamic game insights, such as win probability, in real time.

---

## 🚀 Features

- ⚡ Real-time ingestion of NBA game events (simulated streaming)
- 🔄 Stream processing & feature extraction
- 🧠 Machine learning model for in-game predictions (e.g., win probability)
- 📊 Live dashboard for visualizing game state and predictions
- 🛠️ Modular architecture with separate components for data, model, and visualization

---

## 🛠️ Tech Stack

- Kafka (for streaming data processing pipeline)
- Grafana (for dashboarding and visualizations)
- PostgreSQL / Redis (for storage)
- Docker (for deployment)

**ML Modeling**

- Python (scikit-learn, XGBoost, or PyTorch)
- Feature engineering with Pandas / PySpark

---

## 📦 Installation & Setup

```bash
# Clone the repo
git clone https://github.com/keithy1012/nba-streaming-data.git
cd nba-realtime-insights

# Set up Python virtual environment
python -m venv venv
source venv/bin/activate  # On Windows use `venv\Scripts\activate`

# Install backend dependencies
pip install -r requirements.txt

# Service Deployment
docker-compose up -d

# Start the Data Streaming Processor
python stream_ingestion/live_producer.py

# Run the Data Processing Pipeline
python stream_processing/processor.py


```
