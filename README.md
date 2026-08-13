# 🚀 TelemetryCore — IoT Telemetry Ingestion & Visualization Platform

TelemetryCore is a high-performance, real-time telemetry ingestion and visualization platform designed to stream, process, and display high-frequency data from edge devices and robotics fleets with minimal latency. Built on a microservices architecture, it utilizes Protocol Buffers over high-speed transport streams to deliver compressed binary sensor data into a FastAPI ingestion gateway. Data is instantly buffered through Redis Streams for real-time anomaly detection and low-latency WebSocket broadcasting, while persistent time-series metrics are saved to TimescaleDB for historical analytics. The platform features an interactive 3D dashboard built with React and Three.js, giving developers and operators real-time spatial heatmaps and device telemetry insights in an enterprise-ready, containerized environment.

## 🏗️ Architecture Overview

```
     📱 IoT Devices
        ╰──╮
           │ Protobuf/HTTP
           │
      🔥 FastAPI Gateway
      ┌────────────────────────────────┐
      │  POST /api/v1/telemetry        │
      │  • Accepts binary data         │
      │  • Zero buffering              │
      │  • Stream to Redis immediately │
      └────────────────────────────────┘
           │
           │ Raw bytes
           ↓
      📦 Redis Stream
      ┌────────────────────────────────┐
      │  Bounded Buffer (max 100K)     │
      │  • Decoupling layer            │
      │  • Disk-backed persistence     │
      │  • XREAD blocking reads        │
      └────────────────────────────────┘
           │
           │ XREAD
           ↓
      ⚙️ Stream Consumer
      ┌────────────────────────────────┐
      │  Background Task (app/main.py) │
      │  • Polls Redis Stream          │
      │  • Decodes Protobuf            │
      │  • Per-stream offset tracking  │
      │  • Error resilience + retry    │
      └────────────────────────────────┘
           │
           │ INSERT
           ↓
      🗄️ PostgreSQL/TimescaleDB
      ┌────────────────────────────────┐
      │  • telemetry_readings (hyper)  │
      │  • alerts (anomalies)          │
      │  • devices (metadata)          │
      │  • 1-day time partitioning     │
      └────────────────────────────────┘
```

### 📦 Components

#### 1. **🔥 Gateway** (`gateway/app/main.py`)
- FastAPI async HTTP service
- Ingestion endpoint: `POST /api/v1/telemetry`
- Accepts binary Protobuf or raw bytes
- Pushes to Redis Stream immediately (no buffering in memory)
- Response: `{"status": "ok", "parsed": true/false}`

#### 2. **📪 Redis Stream** (via docker-compose)
- Bounded buffer (max 100K entries)
- XADD with maxlen=100K to prevent memory overflow
- Persists to disk (if configured in docker-compose)
- Acts as a decoupling point between ingestion and processing

#### 3. **⚙️ Consumer** (`gateway/app/consumer.py`)
- Background task (runs in FastAPI lifespan)
- Polls Redis Stream with `XREAD` blocking reads (1s interval)
- Decodes Protobuf (future: full implementation)
- Writes decoded telemetry to TimescaleDB
- Per-stream offset tracking for multi-stream support

#### 4. **🗄️ Database** (`db/schema.sql`)
- **TimescaleDB** (PostgreSQL extension)
- `telemetry_readings` hypertable: time-series partitioned on timestamp
- `alerts` table: anomaly detection results
- `devices` table: device metadata
- Indexes: device_id + timestamp for fast queries

### ⚙️ Configuration

Settings are centralized in `gateway/app/config.py` using Pydantic:

**📪 Redis:**
- `REDIS_HOST`, `REDIS_PORT` (default: localhost:6379)
- `REDIS_STREAM_KEY` (default: "telemetry:stream")
- `REDIS_MAX_STREAM_LEN` (default: 100,000)

**🗄️ Postgres/TimescaleDB:**
- `POSTGRES_HOST`, `POSTGRES_PORT`, `POSTGRES_DB`, `POSTGRES_USER`, `POSTGRES_PASSWORD`
- `POSTGRES_DSN` (full connection string, takes precedence)

Set via environment variables or `.env` file.

### 📋 Protocol Buffer

**Message:** `MetricReading` (proto/telemetry.proto)
```protobuf
message MetricReading {
  string device_id = 1;
  uint64 timestamp = 2;              // Unix epoch in milliseconds
  float temperature = 3;
  float voltage = 4;
  Vector3 acceleration = 5;
  Vector3 gyro = 6;
  map<string, string> metadata = 7;  // Optional key-value tags
}

message Vector3 {
  float x = 1;
  float y = 2;
  float z = 3;
}
```

---

## 🛠️ Development Setup

### ✅ Prerequisites
- Python 3.10+
- Docker & Docker Compose
- Redis (or via docker-compose)
- PostgreSQL/TimescaleDB (or via docker-compose)

### 📦 Install Dependencies

```bash
# Activate virtual environment
source .venv/bin/activate

# Install gateway requirements
pip install -r telemetry-core/gateway/requirements.txt
```

### 🗄️ Database Setup

```bash
# Start infrastructure (Redis + PostgreSQL)
cd telemetry-core
docker-compose up -d

# Initialize schema
psql -h localhost -U postgres -d telemetry -f db/schema.sql
```

### 🚀 Run the Gateway

```bash
cd telemetry-core/gateway

# Start the ingestion server (port 8000)
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

The app logs consumer progress and telemetry ingestion in real-time.

---

## ✅ Testing

### 📤 Send a Test Telemetry Reading

```python
import requests
from app.pb import telemetry_pb2

# Create a Protobuf message
msg = telemetry_pb2.MetricReading(
    device_id="device_001",
    timestamp=1691234567000,  # milliseconds
    temperature=23.5,
    voltage=12.0,
)

# Send to gateway
response = requests.post(
    "http://localhost:8000/api/v1/telemetry",
    data=msg.SerializeToString(),
)
print(response.json())  # {"status": "ok", "parsed": true}
```

### 📊 Query Telemetry from Database

```bash
psql -h localhost -U postgres -d telemetry

SELECT device_id, timestamp, temperature, voltage 
FROM telemetry_readings 
WHERE device_id = 'device_001'
ORDER BY timestamp DESC
LIMIT 10;
```

---

## 📈 Project Status

- ✅ Protobuf schema (MetricReading)
- ✅ FastAPI gateway with binary ingestion
- ✅ Redis Stream buffering
- ✅ Stream consumer with background task
- ✅ PostgreSQL/TimescaleDB schema & client
- 🚧 Full Protobuf decoding in consumer
- 🚧 Anomaly detection pipeline
- 🚧 React frontend with WebSocket subscriptions
- 🚧 Visualization (Three.js 3D)

---

## 🎯 Next Steps

1. **🔗 Consumer DB Integration**: Wire PostgresClient into consumer for persistent writes
2. **🚨 Anomaly Detection**: Implement rules engine for temperature/voltage/acceleration thresholds
3. **🧪 Integration Tests**: End-to-end tests with Pytest + test data
4. **🎨 Frontend**: React + WebSocket consumer for real-time dashboards
5. **⚡ Load Testing**: Locust for simulating thousands of devices

---

## ⚡ Memory & Performance Notes

- **💾 Zero buffering in app memory**: Telemetry flows directly Redis → DB
- **📪 Redis bounded**: 100K max entries prevents runaway memory growth
- **📊 Hypertable partitioning**: 1-day chunks for efficient time-series queries
- **🔌 Connection pooling**: 5-20 Postgres connections (asyncpg)
- **⚙️ Async I/O**: Non-blocking Redis/DB operations

---

## 📚 References

- 🗄️ [TimescaleDB Docs](https://docs.timescale.com/)
- 🔥 [FastAPI](https://fastapi.tiangolo.com/)
- 📦 [Protocol Buffers](https://developers.google.com/protocol-buffers)
- 📪 [Redis Streams](https://redis.io/docs/data-types/streams/)
