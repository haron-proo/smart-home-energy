
### 🔌 Real-Time IoT Appliance Simulator (`appliance_simulator.py`)

The **IoT Appliance Simulator** is a high-fidelity python engine designed to simulate real-time energy consumption behaviors of a smart luxury villa. It acts as the primary data generator for the entire pipeline, emitting telemetry events into Apache Kafka.

#### Key Architectural Features:
* **Hybrid Database & Local Cache Management:** Implements a localized 60-second in-memory cache synchronized with PostgreSQL (`devices_status` and `user_preferences` tables) to dynamically track registered smart devices, physical baselines, and active user interventions (e.g., manual overrides and eco-mode status).
* **Smart Eco-Mode Logic:** Adapts device states (`ON`/`OFF`) and scales down wattage consumption on-the-fly depending on whether the system-wide **Eco Mode** is enabled.
* **Context-Aware Simulation Modalities:** Simulates behavioral and environment variables including:
  * **Temporal Patterns:** Sleeping hours, heat peaks, and evening rushes.
  * **Zone Dynamics & Occupancy Weighting:** Variable occupancy probabilities across specific house zones (Living Room, Kitchen, Master Bedroom, Outdoor, Garage).
* **Data Quality Chaos Engine (Anomalies):** Programmatically injects data quality issues at a controlled rate to simulate real-world hardware and telemetry failures (e.g., negative values, sudden power spikes, `LOST_SIGNAL` states, and duplicate packets) to stress-test downstream Spark cleansing logic.
* **Synchronized Time Engine:** Strictly synchronized to local system time (`Asia/Riyadh`) instead of standard UTC to match system and UI logging expectations.

#### Event Schema (Kafka Topic: `energy_events`):

```json
{
  "timestamp": "2026-07-16 03:41:58",
  "house_type": "Smart_Luxury_Villa",
  "currency": "YER",
  "zone": "Living_Room",
  "device_id": "LV_Smart_AC",
  "device_type": "AC",
  "is_room_occupied": true,
  "power_consumption_watts": 845.2,
  "status": "ON"
}

```

---

### 🧠 Intelligent AI Report Engine (`ai_report_generator.ipynb`)

This module executes the predictive analytics and anomaly detection logic of the smart home system. It integrates a pre-trained machine learning model with real-time operational database tables to generate actionable, cost-aware insights.

#### Key Architectural Features:

* **Dynamic Offline Package Management:** Dynamically verifies and installs crucial statistical and database libraries (`scikit-learn`, `pandas`, `joblib`, `psycopg2`) to an isolated local volume, ensuring total offline reliability.
* **ML-Powered Dynamic Thresholding:** Utilizes a trained `DecisionTreeRegressor` model to predict the *expected* energy consumption of any device based on the current hour of the day, location (zone), and device type.
* **Intelligent Anomaly Detection:**
* Compares real-time sliding window aggregates (`spark_windowed_energy`) with the ML model's predicted thresholds.
* Ingests dynamic electricity pricing rates (`electricity_prices`) mapped directly to the local time hour.
* Triggers smart alerts if a device's actual consumption deviates from its predicted "normal behavior" by more than **25%** (ignoring devices manually forced `OFF`).


* **Financial Impact & Recommendation Modeling:** Generates tailored, localized recommendations indicating precisely which device in which zone is violating normal behavior, computes waste in kilowatt-hours (kWh), and estimates exact financial savings in Yemeni Rials (YER/hour).
* **Target Output Table:** Upserts final rich JSON-formatted report metadata into PostgreSQL `ai_home_reports`.

---

### 🏛️ Unified DWH Batch Pipeline (`spark_dwh_batch.ipynb`)

This notebook constitutes the core ETL batch processor of the **Gold Layer / Data Warehouse (DWH)**, managing both hourly incremental analytics and historical monthly reporting.

#### Key Architectural Features:

* **Incremental Analytical Upserts (Part 1):**
* Reads incrementally from the Silver parquet directory (`historical_parquet`) using Spark streaming metadata checkpointing.
* Enriches energy records by engineering time keys (hours, dates, peak periods) and broadcasting the database's `electricity_prices` lookup table.
* Computes granular aggregations (active, wasted, and peak-hour energy consumption) as well as data quality failure metrics (`sensor_fault` and `lost_signal` counts).
* Implements a **Safe Staging Upsert Pattern**: Computes batches, overwrites a temporary PostgreSQL staging table, and executes an atomic `ON CONFLICT DO UPDATE` query to guarantee zero-duplicate warehousing.


* **Accumulated Monthly Reporting (Part 2):**
* Processes historical files globally to group and average consumption metrics by year-month (`YYYY-MM`).
* Aggregates total monthly energy consumption (kWh) and cumulative costs (YER) using high-speed broadcast joins.
* **Storage Targets:** Injects structured analytics into `dwh_energy_analytics` and monthly aggregated trends into `dwh_monthly_energy_reports`.



---

### 📊 Historical Compaction & Aggregation (`process_parquet_batch.ipynb`)

A lightweight batch utility engineered to run historical data compactions and structural summaries inside the Jupyter container.

#### Key Architectural Features:

* **Auto-Resolution of Evolving Schemas:** Dynamically inspects parquet schemas to handle structural column naming differences (such as mapping `power_consumption_watts` or `avg_power_watts` into a unified internal schema).
* **Data Cleansing Guardrails:** Strips null records, filters out non-numeric entries, and casts the power metrics cleanly to `DoubleType` before distributed aggregation begins.
* **Analytical Aggregations:** Computes cumulative energy usage metrics (overall average wattage, peak power spikes, and audit record counts) grouped strictly by `zone` and `device_type`.
* **Fast Dashboard Integration:** Overwrites the aggregated analytical summaries directly to the `historical_analytics_summary` table in PostgreSQL to feed low-latency operational dashboards in Metabase.

---

### 🤖 Predictive ML Training Pipeline (`ml_data.ipynb`)

This notebook is the dedicated model training pipeline of the Smart Home ecosystem, designed to train and serialize predictive power consumption models offline.

#### Key Architectural Features:

* **100% Air-Gapped/Offline Dependencies:** Configured with specific pip flags (`--no-index --find-links`) to install heavy scikit-learn wheel dependencies (`numpy`, `pandas`, `scikit-learn`, `scipy`, `joblib`) completely offline from local storage volumes.
* **Spark-to-Pandas Bridging:** Leverages Apache Spark to read large-scale windowed streaming histories from PostgreSQL via JDBC, processes the records using Spark SQL to extract historical hour-of-day features, and converts the resulting dataset to a Pandas DataFrame for training.
* **Model Feature Engineering:** Transforms categorical variables (`zone`, `device_type`) into high-dimensional numerical vectors using One-Hot Encoding (`get_dummies`), ensuring a robust training matrix.
* **Regression Modeling:** Builds, trains, and evaluates a `DecisionTreeRegressor` (with controlled depth constraints to prevent overfitting).
* **Model Serialization:** Saves the trained predictor and its corresponding feature map to a portable joblib binary (`energy_model.pkl`) to be served directly by the AI Report Generator.

---

### 📡 Lightweight Notebook Executor API (`app.py`)

A high-performance FastAPI microservice acting as an orchestration bridge. It allows external schedulers (such as Apache Airflow) to securely trigger heavy PySpark Jupyter Notebooks inside the Spark container without encountering permission, performance, or timeout limitations.

#### Key Architectural Features:

* **Asynchronous Execution Pattern:** Utilizes FastAPI’s native `BackgroundTasks` to handle notebook executions. When a request is received, the API schedules the job and immediately returns a `200 OK (queued)` response to Airflow in milliseconds, avoiding HTTP gateway timeouts on long-running ETL processes.
* **Deterministic Isolation via Papermill:** Programmatically spins up a clean `papermill` subprocess to execute the target notebook. It keeps the original templates untouched while writing execution results, parameters, and console traces to a dedicated `output_*.ipynb` file.
* **Robust Path Resolution:** Validates resource existence locally inside the Docker volume boundary before starting execution, throwing a clean `404 HTTPException` if the target notebook file is missing.
* **API Exposure Point:** `POST http://localhost:8000/run-notebook`
* **JSON Payload Schema:**

```json
{
  "notebook_name": "ai_report_generator.ipynb"
}

```

---

### ⚡ Real-Time PySpark Stream Processing Engine (`spark_stream.py`)

This is the central backbone of the real-time processing layer, operating 24/7 as a PySpark Structured Streaming job. It continuously pulls telemetry from Apache Kafka, sanitizes context-dependent anomalies, computes rolling time-window aggregates, and upserts them to the operational database.

#### Key Architectural Features:

* **Air-Gapped Offline Setup:** Automatically bootstraps database connection drivers (`psycopg2`) at runtime in a local container directory if they are missing, ensuring seamless offline container initializations.
* **Stream Optimization & Memory Guardrails:** * Limits memory overhead by filtering input events to only process data within a rolling 10-minute real-time window relative to the system clock.
* Employs a 15-minute Watermark (`withWatermark`) to gracefully handle late-arriving IoT packets and discard old state data from Spark's memory.


* **Context-Aware Physical Sanity Filters:** Eliminates anomalies by filtering out invalid data based on physical device characteristics:
* Discards telemetry labeled with error states (`LOST_SIGNAL`, `SENSOR_FAULT`).
* Enforces maximum load limitations based on device types (e.g., capping ACs at 6000W, and Smart Bulbs/Lighting at 500W) to block corrupted extreme values.
* Drops duplicate entries dynamically using key combinations (`timestamp`, `device_id`).


* **Sliding Window Aggregation:** Computes rolling 5-minute windows sliding every 1 minute to capture short-term load averages (`avg_power_watts`) and aggregate loads (`total_power_watts`) per zone and device type.
* **High-Speed Partitioned Upserts:**
* Uses PySpark's `.foreachPartition()` pattern to establish pooled database connections per partition, drastically reducing database connection overhead.
* Writes to PostgreSQL using an optimized native SQL `INSERT ... ON CONFLICT (zone, device_type, window_end) DO UPDATE` query.


* **Automated Database Purge Strategy:** To prevent PostgreSQL bloat, the engine triggers an automatic cleanup transaction every 5 batches, pruning sliding window records older than 10 minutes from the database.
* **Fault-Tolerant Checkpoint Healing:** Automatically resolves Windows/POSIX file-locking conflicts by dynamically renaming or assigning unique timestamp suffixes to checkpoint metadata folders if locks are detected.

---

### 🗄️ Relational Database Schema (`schema.sql`)

The PostgreSQL database acts as the central query and storage engine. The schema is highly optimized using specific indexes to support both high-speed real-time ingestion from PySpark Streaming and low-latency aggregate reads from Metabase.

#### Database Architecture & Table Breakdown:

1. **`spark_windowed_energy` (Real-Time Storage):** Holds the active 5-minute sliding window aggregates calculated by the PySpark streaming engine. Enforces a `UNIQUE` constraint on (`zone`, `device_type`, `window_end`) to support reliable upserts.
2. **`dwh_energy_analytics` (Core DWH Batch):** The primary star-schema analytical table. It tracks energy consumption (kWh), monetary costs (YER), wasted energy due to unoccupied rooms, and data quality issues.
3. **`historical_analytics_summary` (Aggregated Cache):** A compressed cache table designed to quickly feed high-level widgets on the dashboard, keeping track of overall averages, peak metrics, and records analyzed per device and zone.
4. **`dwh_monthly_energy_reports` (Financial Ledger):** Houses the consolidated monthly electricity consumption and estimated costs in Yemeni Rials (YER) for historical reporting.
5. **`pc_energy_analytics` (Device Ingestion Audit):** A staging and auditing table used to log historical device behaviors, peaks, read counts, and data quality structures prior to warehousing.
6. **`ai_home_reports` (AI Insights Hub):** A specialized table storing ML-driven model metadata, status levels (`NORMAL`, `WARNING`, `CRITICAL`), and nested detailed JSON payloads (`critical_alerts`, `ai_recommendations`) for personalized home safety messages.

#### Query Optimization & Indexing Strategy:

* **`idx_ai_reports_timestamp`:** High-speed index on `report_timestamp DESC` to fetch the latest AI predictions and recommendations in milliseconds.
* **`idx_analytics_date_zone`:** A composite index on `(report_date, zone)` optimized for Metabase dashboard filters that query specific regions over historical timeframes.
* **`idx_spark_windowed_query`:** Index on `(zone, device_type, window_end DESC)` designed to speed up `DISTINCT ON` queries executed by the AI Report Generator to fetch the latest device states.

---

### 🚀 Windows Automation Scripts (`.bat` Launchers)

To abstract command-line complexities for end-users, the project includes a suite of lightweight, automated Windows batch scripts. These scripts manage environments, launch microservices, and initiate Docker containers.

#### 1. Real-Time Simulator Orchestrator (`run_simulator.bat`)

```batch
Role: Launches the simulated smart luxury villa's device generator.
Flow:
- Configures terminal encoding to UTF-8 (chcp 65001) for multilingual log rendering.
- Automatically activates the local Python virtual environment (venv).
- Navigates to the /ingestion directory and kicks off appliance_simulator.py.

```

#### 2. Infrastructure Deployment Engine (`run_docker.bat`)

```batch
Role: Provisions and boots the entire containerized architecture.
Flow:
- Automates the invocation of Docker Compose with the project-specific configuration.
- Runs the stack in detached background mode (-d) to keep the terminal free.
- Automatically queries and prints "docker ps" to verify container health at runtime.

```

#### 3. Dashboard Web Server Launcher (`run_dashboard.bat`)

```batch
Role: Spins up the local monitoring dashboard API and opens the UI.
Flow:
- Activates the virtual environment and starts the FastAPI service under dashboard/app.py.
- Programmatically forces the client's default web browser to open the dashboard URL:
  [http://127.0.0.1:9066/dashboard](http://127.0.0.1:9066/dashboard)

```

#### 4. Environment Dependencies File (`requirements.txt`)

Contains all critical local development libraries (e.g., `kafka-python`, `psycopg2-binary`, `fastapi`, `uvicorn`, `pandas`, `scikit-learn`) ensuring repeatable environment setups via:

```bash
pip install -r requirements.txt

```

---

### 🐳 Containerized Infrastructure Stack (`docker-compose-clean.yaml`)

The entire multi-service ecosystem is orchestrated using **Docker Compose** within an isolated bridge network (`smart-home-net`). The stack is meticulously tuned for zero-configuration startup, offline capability, absolute persistent storage, and synchronized time-zones.

#### Key Architectural Services:

1. **`postgres` (Central Storage & Airflow Backend):**
* **Engine:** Postgres 14 (Alpine-based).
* **Persistence:** Mounts local `./postgres-data` to guarantee that IoT events, metrics, and Airflow states survive container restarts indefinitely.
* **Network Exposure:** Exposed locally on port `5433` (mapped from standard `5432` internally) to prevent port conflicts with any pre-existing Postgres installations on the host.


2. **`kafka` (Streaming Ingestion Broker):**
* **Engine:** Apache Kafka 3.7 (Kraft Mode - no Zookeeper required).
* **Networking:** Defines dual listeners. `PLAINTEXT://localhost:9092` for the host's python simulator, and `INTERNAL://smarthome-kafka:29092` for inter-container stream consumption by Spark.


3. **`spark-notebook` (Jupyter & Execution Bridge API):**
* **Engine:** PySpark-Notebook (Spark 3.5.0).
* **Bootstrapping Script:** Upon initialization, it dynamically installs `fastapi`, `uvicorn`, and `papermill` completely offline. It immediately spawns the execution API (`app.py`) on port `8000` in the background, and then launches the Jupyter Notebook daemon.
* **Volume Mapping:** Maps `.` (project root) into `/home/jovyan/work` so all local notebook edits are reflected inside the container instantly.


4. **`metabase` (Operational BI Dashboard):**
* **Engine:** Metabase v0.46.6.
* **Integration:** Configured out-of-the-box using environment variables to connect directly to the `smarthome_energy` Postgres database on port `3000` to enable immediate metric visualization.


5. **`airflow-webserver` & `airflow-scheduler` (Pipeline Orchestrators):**
* **Engine:** Apache Airflow 2.9.1.
* **Execution Pattern:** Configured using the `LocalExecutor` with metadata stored on Postgres (`airflow_db`).
* **Bridge Setup:** Mounts the local `./airflow/dags` directly. Solves the traditional "Airflow executing Spark inside Docker" permission bottleneck by simply requesting the `spark-notebook` API service to execute notebooks on its behalf via REST calls.


6. **`spark-energy-streamer` (Dedicated Stream Runner):**
* **Engine:** Spark Submit Client.
* **Operational Pattern:** Configured with `restart: always` to run as a **daemon service (24/7)**. On boot, it automatically triggers `spark-submit` with the required coordinate packages (`spark-sql-kafka` and `postgresql:42.6.0`) to execute and monitor `spark_stream.py` without requiring manual trigger interventions.



---

### ⏰ Riyadh Timezone Standardization (`Asia/Riyadh`)

Across all containers, the timezone environment parameter is explicitly locked:

```yaml
environment:
  TZ: Asia/Riyadh

```

---

### 🌐 Custom FastAPI Web Dashboard Architecture (`/dashboard`)

The User Dashboard acts as the interactive control room of the Smart Luxury Villa. Built on FastAPI, it seamlessly bridges operational database controls, predictive AI threshold checks, dynamic budget alerting in Yemeni Rials (YER), and secure interactive analytics embedded directly from Metabase.

#### 📂 Core Components & Routing Modules:

1. **`config.py` (Application Configurations):**
* Manages connection URIs pointing directly to the exposed host PostgreSQL port (`5433`).
* Loads high-security configuration variables for Metabase iframe token signing (`METABASE_SITE_URL`, `METABASE_SECRET_KEY`, and `METABASE_DASHBOARD_ID`).


2. **`routers/analytics.py` (The Intelligent Real-Time & Batch Viewer):**
* **Real-Time View (`/dashboard`):** * Computes real-time dynamic threshold violations by comparing active Spark streaming window loads (`spark_windowed_energy`) with historical user limits.
* Evaluates budget performance by matching cumulative month-to-date costs from the DWH (`dwh_energy_analytics`) against the user's defined monthly budget, throwing warning banners if the threshold is breached.
* Signs highly secure, HMAC-SHA256 encrypted single-use JSON Web Tokens (JWT) to embed Metabase dashboards via safe sandboxed iframes without exposing database credentials to the client.


* **Cached Historical View (`/historical-reports`):** Serves extremely low-latency historical summary reports pulled directly from the compiled database cache (`historical_analytics_summary`).
* **Batch Analytics (`/dashboard/batch-analytics`):** Renders clean tabular overviews of cold-path aggregate statistics grouped by zone and device type.


3. **`routers/devices.py` (Device Command Center):**
* **`GET /devices-panel`:** Fetches and displays a tabular inventory of all home appliances and their active execution states.
* **`POST /devices-panel/control`:** Allows administrators to forcefully override device states by publishing `AUTO`, `ON`, or `OFF` states directly to PostgreSQL (which are subsequently reflected in downstream simulator data feeds).
* **`POST /devices-panel/add`:** Enables on-the-fly registration of new appliances with baseline wattage parameters and criticality flags, preventing duplicate keys using SQL `ON CONFLICT DO UPDATE` queries.


4. **`routers/preferences.py` (User Boundaries & Financial Constraints):**
* Controls the primary budget ceilings and activates energy-saving eco-modes (`user_preferences`).
* Stores appliance-specific operational pricing thresholds (`device_thresholds`) used by downstream AI anomaly-detection modules.


5. **`app.py` (Microservice Bootloader):**
* Serves as the microservice entrypoint, mounting local static visual assets (CSS/JS) and modular router prefixes.
* Automatically forwards index root requests (`/`) directly to the main `/dashboard` to optimize end-user accessibility. Runs locally under port `9066`.



```

```