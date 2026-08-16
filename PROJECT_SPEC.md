# TelemetryCore — Project Specification & Architecture Blueprint

## 1. Project Vision & Core Objective
**TelemetryCore** is an enterprise-grade, real-time IoT telemetry ingestion, anomaly detection, and 3D visualization platform. Beyond acting as a high-performance data pipeline, the platform features an immersive, animation-integrated full-stack web application designed for maximum user engagement and operational clarity. 

To lower the barrier to entry and highlight technical execution, the application features an interactive **Public Showcase & Live Demo Portal**, allowing any visitor to explore real-time hardware telemetry streams without requiring authentication or physical device ownership.

---

## 2. Target Audience & User Experience (UX) Goals
* **Engineers & Operators:** Need low-latency metric sparklines, spatial 3D device orientations (gyro/accelerometer), thermal heatmaps, and instant anomaly logs.
* **General Visitors / Showcase Explorers:** Need an intuitive, visually stunning landing page with smooth transitions, interactive mock data toggles, and an engaging "Digital Twin" preview.
* **Design Aesthetic:** Dark-mode high-density industrial dashboard blended with smooth, modern web animations (similar to advanced aerospace or modern SaaS control centers).

---

## 3. High-Level System Architecture
+-------------------------------------------------------------------------------------------------+
|                                     PUBLIC SHOWCASE & LANDING PAGE                              |
|                          (Interactive 3D Preview, Live Metrics, Public Demos)                   |
+-------------------------------------------------------------------------------------------------+
|
v
+------------------------+        +------------------------------+        +-----------------------+
|  Edge Devices / Sims   | -----> |  FastAPI Ingestion Gateway   | -----> |  Redis Streams (RAM)  |
|  (Protobuf / HTTP)     |        |  (Async / 202 Accepted)      |        |  (Bounded Ring Buffer)|
+------------------------+        +------------------------------+        +-----------------------+
|
v
+-----------------------+
|  Stream Workers &     |
|  Consumer Groups      |
+-----------------------+
/

v                   v
+-----------------------+   +-------------------+
| TimescaleDB / Postgres|   | WebSocket Server  |
| (Historical Metrics)  |   | (Live Broadcast)  |
+-----------------------+   +-------------------+
|
v
+-------------------+
| React + Three.js  |
| Frontend Dashboard|
+-------------------+


---

## 4. Frontend & Full-Stack Architecture

### A. Tech Stack
* **Framework:** React + Vite (or Next.js for server-side rendering and SEO-optimized public landing pages).
* **Styling:** Tailwind CSS + `shadcn/ui` for modular, accessible, industrial-grade components.
* **Animations:** Framer Motion for smooth UI micro-interactions, page transitions, and status change morphs.
* **3D Rendering:** React Three Fiber (R3F) & Drei for real-time 3D device orientation models and spatial heatmaps.
* **State & Real-Time Sync:** Zustand for global state management; native WebSockets with auto-reconnection logic for live telemetry streams.

### B. User Flow & Feature Breakdown
1. **Public Showcase / Landing Page (`/`):**
   * High-impact hero section featuring a live WebGL 3D preview model reacting to simulated physics.
   * Interactive "Try the Demo" sandbox allowing visitors to trigger virtual hardware spikes (e.g., thermal overload, high vibration) and watch the UI react.
   * Live global stats ticker (Total messages processed, active nodes, system latency).
2. **Core Dashboard (`/dashboard`):**
   * **Fleet Navigator Sidebar:** Real-time status cards (Nominal, Warning, Critical) with search and filter capabilities.
   * **3D Digital Twin Viewport:** Renders the selected device mesh rotating live via incoming IMU Gyro vectors ($X, Y, Z$), color-coded dynamically by temperature gradients.
   * **High-Frequency Metrics:** Low-latency rolling charts for temperature, voltage, and multi-axis vibration.
   * **Event & Anomaly Terminal:** Live scrolling log feed tracking parser states and automated threshold warnings.

---

## 5. Backend & Data Pipeline Architecture

### A. Tech Stack
* **Ingestion Gateway:** FastAPI (Python, async/await) optimized for high concurrency.
* **Serialization Protocol:** Protocol Buffers (`telemetry.proto`) for minimal payload size and maximum parsing speed.
* **In-Memory Buffer:** Redis Streams (`XREADGROUP`) ensuring zero-data-loss consumer group processing and decoupled writes.
* **Persistence Layer:** TimescaleDB (PostgreSQL extension) utilizing hypertable partitioning for time-series metrics, paired with `asyncpg` for high-performance bulk inserts (`executemany`).

---

## 6. Phased Development Roadmap

* **Phase 1: Core Ingestion & Storage Pipeline (Current)**
  * [x] Protobuf data contract definition (`telemetry.proto`)
  * [x] FastAPI async ingestion endpoint (`POST /api/v1/telemetry`)
  * [x] Redis stream integration & consumer group worker logic (`StreamConsumer`)
  * [x] TimescaleDB/Postgres bulk insert client (`PostgresClient`)
* **Phase 2: WebSocket & Real-Time Broadcasting Layer**
  * [ ] Build WebSocket hub to relay buffered stream metrics directly to connected clients.
  * [ ] Implement basic anomaly detection worker rules (e.g., thermal threshold alerts).
* **Phase 3: Frontend Showcase & Landing Page**
  * [ ] Set up React + Vite project structure with Tailwind CSS and Framer Motion.
  * [ ] Design and code the public showcase landing page and interactive sandbox.
* **Phase 4: 3D Digital Twin Dashboard Integration**
  * [ ] Implement React Three Fiber viewport for real-time 3D device rotation.
  * [ ] Connect frontend components to live WebSocket streams for real-time telemetry updates.