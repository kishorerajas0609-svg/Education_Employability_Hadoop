# EDU-PREDICT — Education Employability Hadoop Platform

EDU-PREDICT is a demonstration platform for reviewing student academic and career-readiness indicators. The repository combines a responsive browser-based dashboard, a Python FastAPI service, Hadoop HDFS integration, and a script that reports dataset and model benchmark information.

> **Demo / verification note:** The checked-in `spark_employability_analysis.py` currently reads and counts a local CSV and prints predefined class/model metrics. It does **not** create a Spark session, submit Spark MLlib jobs, ingest data into HDFS, or train/evaluate models. Treat the displayed benchmark figures as configured demo values, not results reproduced by that script. Likewise, the repository listing does not include the referenced million-row CSV, so data-backed endpoints need that dataset supplied locally.

## Contents

- [Features](#features)
- [Architecture](#architecture)
- [Repository files](#repository-files)
- [Requirements](#requirements)
- [Setup and run](#setup-and-run)
- [Configuration and data](#configuration-and-data)
- [API overview](#api-overview)
- [Dataset and benchmark figures](#dataset-and-benchmark-figures)
- [Limitations and security](#limitations-and-security)
- [Troubleshooting](#troubleshooting)

## Features

- **Student records:** paginated CSV-backed records, search and filters, student details, and CSV export.
- **Employability assessment:** a weighted score based on CGPA, attendance, projects, internships, certifications, skills, and backlogs, with strengths and improvement suggestions.
- **Skill-gap assessment:** compares a selected skill set with the catalogued role requirements and suggests next steps.
- **Placement dashboard and reports:** summary metrics, model comparison charts, and CSV/JSON/report downloads.
- **Hadoop integration:** reports local Hadoop daemon status, lists files in the configured HDFS directory, and accepts uploads for HDFS when the cluster is available (otherwise files are staged locally).
- **Browser interface:** responsive dashboard with login/demo flows, dark and light themes, records, prediction, skill-gap, results, and settings views.

The role catalog and endpoints are defined in `main.py`; the browser UI is implemented in `index.html`, `style.css`, and `script.js`.

## Architecture

| Layer | Implementation | Notes |
| --- | --- | --- |
| Frontend | HTML, CSS, vanilla JavaScript | `index.html` loads `style.css` and `script.js`; API base URL is `http://127.0.0.1:8000`. |
| Backend | Python FastAPI served by Uvicorn | `main.py`; interactive API documentation is available at `/docs`. |
| Data source | Local CSV file | Used for student listing, lookup, and export. |
| Distributed storage | Apache Hadoop HDFS | Target directory: `/education_employability/raw`; HDFS commands are configured for Windows. |
| Analytics | `spark_employability_analysis.py` | Currently a local CSV counter/reporting demo, not an executing Spark pipeline. |

## Repository files

- `main.py` — FastAPI app: demo authentication, Hadoop/HDFS status and upload endpoints, student APIs, scoring, skill-gap evaluations, metrics, and exports.
- `index.html` — public landing/about/login screens and placement portal markup.
- `style.css` — responsive dashboard styling and dark/light theme tokens.
- `script.js` — browser navigation, API calls, forms, charts, filtering, and downloads.
- `spark_employability_analysis.py` — local CSV record-count and configured benchmark output.
- `START_ALL.cmd` — Windows convenience launcher for Hadoop, backend, and browser.
- `START_HADOOP.cmd`, `STOP_HADOOP.cmd` — Windows Hadoop start/stop helpers.
- `START_BACKEND.cmd` — Windows Uvicorn helper.
- `RUN_SPARK_ANALYSIS.cmd` — runs the analytics/report script using a project virtual environment path.
- `README.txt` — original plain-text run notes; this README is the expanded project guide.
- `.hintrc` — webhint configuration.

## Requirements

- Windows for the supplied `.cmd` launchers and the current Hadoop command configuration.
- Python 3.10+ recommended, with FastAPI, Uvicorn, Pydantic, and `python-multipart` installed.
- Java and a working Hadoop installation if HDFS features are needed. The scripts and backend expect Hadoop under `C:\hadoop` and the `jps` command on `PATH`.
- The synthetic dataset CSV for data-backed student features. See [Configuration and data](#configuration-and-data).

The frontend uses browser APIs and Google Fonts; a network connection is needed to fetch the external fonts. No Node.js build step is present in the repository.

## Setup and run

### 1. Start the API

From the repository root, create and activate a virtual environment, install dependencies, and start Uvicorn:

```bat
py -m venv .venv
.venv\Scripts\activate
python -m pip install fastapi uvicorn python-multipart pydantic
python -m uvicorn main:app --reload --host 127.0.0.1 --port 8000
```

Then open:

- API health/info: <http://127.0.0.1:8000/>
- Swagger UI: <http://127.0.0.1:8000/docs>
- ReDoc: <http://127.0.0.1:8000/redoc>

`main.py` currently configures HDFS commands for Windows. The API can start without Hadoop, but cluster and HDFS operations will be offline or return errors until Hadoop is installed and running.

### 2. Start Hadoop (optional)

Open Command Prompt and run the Hadoop start script, or use the supplied helper:

```bat
C:\hadoop\sbin\start-dfs.cmd
```

```bat
START_HADOOP.cmd
```

Check the Java daemons with `jps`; the NameNode web UI is at <http://localhost:9870>. The backend checks Hadoop processes and ports including 9870, 9864, 9000, and 8020. Stop the cluster with `STOP_HADOOP.cmd` or `C:\hadoop\sbin\stop-dfs.cmd`.

### 3. Open the frontend

Open `index.html` in a browser. The frontend calls the API at `http://127.0.0.1:8000`, so start the backend first for live data and API-backed functionality. Some UI areas have local/demo fallback data when the API is unavailable; those fallbacks are not evidence of live Hadoop or model execution.

### 4. Use the convenience launchers with care

The checked-in command files refer to a `BACKEND` directory and a `BACKEND\venv` environment, while the repository file listing exposes `main.py` at the repository root. `START_ALL.cmd`, `START_BACKEND.cmd`, and `RUN_SPARK_ANALYSIS.cmd` may therefore need their paths adjusted to match your checkout. The commands in steps 1–3 reflect the root-level `main.py` layout. `START_ALL.cmd` also assumes Hadoop is installed at `C:\hadoop`.

### 5. Run the analytics reporting script

With the dataset in the script's current working directory or next to `spark_employability_analysis.py`, run:

```bat
python spark_employability_analysis.py
```

Or try `RUN_SPARK_ANALYSIS.cmd` after checking its `BACKEND\venv\Scripts\python.exe` path. As noted above, the present Python script prints a report and configured benchmark numbers; it does not run Spark MLlib.

## Configuration and data

### Dataset

The application refers to a file named:

```text
synthetic_employability_form_fields_1000000.csv
```

The expected CSV schema, as read by the backend, has these 16 fields:

```text
student_id,student_name,cgpa,attendance_pct,project_count,internship_count,certification_count,skill_python,skill_java,skill_sql,skill_machine_learning,skill_cloud,skill_web_development,technical_skills_count,employability_score,employability_level
```

Place the CSV where the application can find it, or update `CSV_DATASET_PATH` in `main.py`. Note that `main.py` calculates that path relative to its own location (two parent directories up); with `main.py` at the repository root, this points outside the repository. Adjust it for your actual dataset location before using student-record APIs. The analytics script instead checks the current working directory and then its own directory.

The repository content inspected for this README did not include the CSV. If it is not available locally, dataset-backed endpoints such as `/api/students`, `/api/student/{student_id}`, and `/api/export/students-csv` cannot return the advertised cohort records.

### Hadoop

`main.py` currently sets:

- Hadoop executable: `C:\hadoop\bin\hdfs.cmd`
- HDFS upload directory: `/education_employability/raw`
- Local staging directory: `temp_uploads`

Change these settings in `main.py` for a different installation. The app attempts to stage uploads locally when Hadoop is offline; HDFS upload only occurs when the NameNode checks succeed.

### API and browser configuration

The frontend API base URL is the `API_BASE_URL` constant near the top of `script.js`. The backend enables permissive CORS (`*`) for the demo. Restrict both the API URL and CORS policy appropriately before deployment.

## API overview

The FastAPI application exposes the following main routes. Use `/docs` for request/response schemas and the complete interactive list.

| Method | Route | Purpose |
| --- | --- | --- |
| `GET` | `/` | API status and Hadoop daemon summary. |
| `GET` | `/cluster-status` | Hadoop daemon and port checks. |
| `GET` | `/hdfs-status` | HDFS connectivity check. |
| `GET` | `/hdfs-files` | List files in the HDFS raw directory, or local staged files when Hadoop is offline. |
| `POST` | `/create-hdfs-directory` | Create the configured HDFS directory. |
| `POST` | `/upload` | Stage an uploaded file and attempt to transfer it to HDFS. |
| `POST` | `/auth/login`, `/auth/signup` | Demo login and signup flows. |
| `GET` | `/auth/me`, `/auth/demo-users` | Current demo user and demo-account information. |
| `POST` | `/auth/forgot-password` | Simulated password-reset response. |
| `POST` | `/predict` | Calculate a weighted employability score for a submitted profile. |
| `GET` | `/api/dashboard-stats` | Dashboard cohort metrics. |
| `GET` | `/api/students` | Paginated/searchable dataset records. |
| `GET` | `/api/student/{student_id}` | Look up one student by ID. |
| `GET` | `/api/skill-gap-roles` | List role requirements and recommendations. |
| `POST` | `/api/student-skill-gap` | Assess submitted skills against a target role. |
| `GET` | `/api/aggregate-skill-gaps` | Preconfigured aggregate skill-gap figures. |
| `GET` | `/api/evaluation-metrics`, `/dataset-metrics` | Preconfigured evaluation and dataset/benchmark figures. |
| `GET` | `/api/export/students-csv` | Stream a limited CSV audit export. |

## Dataset and benchmark figures

The code and original project notes describe a synthetic dataset of 1,000,000 profiles (67.3 MB), HDFS block size 128 MB, and eight Spark partitions. The backend also returns configured cohort and model metrics. The reporting script lists these model figures:

| Model | Accuracy | Precision | Recall | F1 | Reported training time |
| --- | ---: | ---: | ---: | ---: | ---: |
| Gradient-Boosted Trees (GBT) | 92.4% | 91.8% | 92.1% | 91.9% | 14.6 s |
| Random Forest | 89.7% | 89.1% | 89.5% | 89.3% | 9.2 s |
| Decision Tree | 84.1% | 83.5% | 83.9% | 83.7% | 3.8 s |
| Logistic Regression | 81.3% | 80.6% | 81.0% | 80.8% | 2.4 s |

The script also prints a configured feature-importance ranking: CGPA (32.5%), internships (20.1%), projects (17.8%), technical skills (14.2%), certifications (9.6%), and attendance (5.8%). These values are hard-coded in the checked-in source and are not calculated or validated by the script. The backend's scoring endpoint is a separate weighted rule-based calculation, not an inference call to a trained model.

## Limitations and security

This repository is currently a local demonstration, not a production-ready deployment:

- Authentication is held in memory, includes hard-coded demo accounts, and has permissive demo token behavior. Restarting the process loses newly registered users and sessions.
- Do not use real or sensitive student information with the demo auth implementation. Passwords are stored as plain text in the in-memory registry; use proper password hashing, persistence, authorization, and secret management before deployment.
- CORS allows all origins, and the upload endpoint has no configured file-size or file-type policy. Restrict origins and validate uploads before exposing the service.
- The HDFS path and commands assume a local Windows Hadoop installation; they are not portable as-is.
- The analytics script is not yet a Spark job, and its displayed model metrics are static. A real evaluation requires implementing the Spark ETL/training flow and documenting the dataset, split, and reproducible evaluation procedure.
- The CSV is not included in the repository listing. Several dashboard and aggregate figures are hard-coded, so they should be verified against a real data source before being treated as institutional statistics.
- `README.txt` describes a 10-month roadmap and eight career tracks, but the inspected backend role catalog and frontend implementation should be treated as the source of implemented behavior; the backend catalog contains six roles.

## Troubleshooting

- **Frontend cannot connect:** confirm Uvicorn is listening on `127.0.0.1:8000`; check `API_BASE_URL` in `script.js`.
- **Student APIs report a missing dataset:** check `CSV_DATASET_PATH` in `main.py`, confirm the CSV exists at that exact path, and ensure its header matches the expected schema.
- **HDFS shows offline:** verify Java/Hadoop installation, `jps`, the configured `C:\hadoop` paths, and NameNode UI at port 9870.
- **A `.cmd` launcher cannot find Python or the app:** inspect its assumed `BACKEND` directory and virtual-environment path; run the explicit root-level Uvicorn command above or update the launcher.
- **The Spark report cannot find the CSV:** run the script from a directory containing the CSV or place the CSV next to the script. Remember this script currently reports configured figures rather than running Spark.
