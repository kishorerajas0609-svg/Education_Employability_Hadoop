=============================================================================
EDU-PREDICT: Big Data Education Employability Platform
=============================================================================

Big Data Architecture:
- Distributed Storage: Apache Hadoop HDFS (/education_employability/raw)
- Processing Engine  : Apache Spark MLlib & Distributed Feature Extraction
- Backend Framework  : Python FastAPI (Uvicorn ASGI Web Server)
- Dataset Scale      : 1,000,000 Synthetic Student Profiles (67.3 MB CSV)
- Frontend           : Responsive Dark Glassmorphism Web Interface

-----------------------------------------------------------------------------
HOW TO RUN USING CMD (STEP-BY-STEP):
-----------------------------------------------------------------------------

OPTION 1: ONE-CLICK LAUNCH (RECOMMENDED)
---------------------------------------
Double-click:
  START_ALL.cmd
This automatically starts:
  1. Hadoop DFS (NameNode and DataNode CMD windows)
  2. FastAPI Uvicorn backend server (Port 8000)
  3. Opens the website (index.html) in your browser


OPTION 2: RUN INDIVIDUAL SERVICES IN CMD
---------------------------------------
1. START APACHE HADOOP (NameNode + DataNode):
   Open Command Prompt (CMD) and run:
     C:\hadoop\sbin\start-dfs.cmd
   Or double-click: START_HADOOP.cmd

   To verify Hadoop is running, in CMD run:
     jps
   You will see:
     - NameNode
     - DataNode
     - Jps

   Open Hadoop NameNode Web UI:
     http://localhost:9870

2. START FASTAPI WEBSERVER:
   Open Command Prompt (CMD) in the project directory and run:
     cd "d:\big data - duplicate\Education_Employability_Website\education_employability_website\BACKEND"
     call venv\Scripts\activate.bat
     uvicorn main:app --reload --port 8000
   Or double-click: START_BACKEND.cmd

   FastAPI Documentation (Swagger UI):
     http://127.0.0.1:8000/docs

3. RUN BIG DATA & SPARK ML EVALUATION SCRIPT:
   Double-click:
     RUN_SPARK_ANALYSIS.cmd
   Or in CMD run:
     python spark_employability_analysis.py

4. STOP HADOOP WHEN FINISHED:
   In CMD run:
     C:\hadoop\sbin\stop-dfs.cmd
   Or double-click: STOP_HADOOP.cmd

-----------------------------------------------------------------------------
WEBSITE SECTIONS OVERVIEW:
-----------------------------------------------------------------------------
1. Home:
   - Big Data project overview, distributed pipeline, and real-time KPIs.
2. Academic Records:
   - Live Hadoop cluster status monitor (NameNode, DataNode).
   - Ingest CSV, PDF, XLSX student records into Hadoop HDFS.
   - Live HDFS file browser for /education_employability/raw.
3. Prediction:
   - Interactive student profile evaluator (CGPA, Attendance, Projects,
     Internships, Certifications, Backlogs, Technical Skills).
   - Instant sample student loader (High, Medium, Growth profiles).
   - Feature score breakdown & strengths/weaknesses diagnosis.
4. Skill Gap Analysis:
   - 8 Career Tracks (Big Data Engineer, Data Analyst, AI/ML, Software, etc.).
   - Interactive skills checklist and role match percentage meter.
   - Comprehensive 10-Month Professional Roadmap with tasks and milestones.
5. Results & Analysis:
   - Distributed Spark ML benchmark on 1,000,000 records.
   - Accuracy: GBT (92.4%), Random Forest (89.7%), Decision Tree (84.1%),
     Logistic Regression (81.3%).
   - Feature importance breakdown and HDFS distributed block specs.
=============================================================================
