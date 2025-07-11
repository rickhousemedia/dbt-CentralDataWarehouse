
Centralized Data Warehouse (AWS RDS PostgreSQL + dbt)

📖 Overview

This repository contains the dbt project for our Centralized Data Warehouse, built on PostgreSQL hosted in AWS RDS.

We use dbt (data build tool) to transform raw operational data into a clean, unified, analytics-ready model. Our approach enforces:
	•	Version-controlled, modular SQL transformations
	•	Clear model layers (staging → marts)
	•	Automated testing and documentation
	•	Secure, scalable hosting on AWS RDS PostgreSQL

⸻

📦 Repository Structure

.
├── models/
│   ├── staging/
│   ├── intermediate/
│   └── marts/
├── seeds/
├── snapshots/
├── macros/
├── analyses/
├── tests/
├── dbt_project.yml
└── README.md


⸻

🎯 Project Goals
	•	✅ Centralize operational data into AWS RDS PostgreSQL
	•	✅ Establish a single source of truth for reporting
	•	✅ Automate data quality checks with dbt tests
	•	✅ Enable scalable, maintainable ELT workflows
	•	✅ Document models for easy onboarding

⸻

⚙️ Tech Stack

Component	Tool / Service
Data Warehouse	AWS RDS PostgreSQL
Transformation	dbt (data build tool)
Orchestration (optional)	Airflow / Prefect / dbt Cloud
Version Control	Git / GitHub / GitLab


⸻

🚀 Setup Instructions

1️⃣ Clone the Repository

git clone https://github.com/your-org/your-repo.git
cd your-repo


⸻

2️⃣ Install dbt

Install the dependencies inlcuding the PostgreSQL adapter:

pip install -r requirements.txt


⸻

3️⃣ Configure AWS RDS Connection

Create or update your profiles.yml (default location: ~/.dbt/profiles.yml):

your_project_name:
  target: dev
  outputs:
    dev:
      type: postgres
      host: your-db-instance.endpoint.amazonaws.com
      user: your-db-username
      password: your-db-password
      port: 5432
      dbname: your-db-name
      schema: your-schema
      threads: 4
      sslmode: require

✅ Replace:
	•	host with your AWS RDS endpoint (e.g., mydb.abcdefg123.us-east-1.rds.amazonaws.com)
	•	user, password, dbname, schema with your credentials

✅ Recommended best practice: Use environment variables to store secrets securely.

⸻

4️⃣ Install Dependencies

If your project uses dbt packages:

dbt deps


⸻

5️⃣ Run Models

dbt run


⸻

6️⃣ Test Models

dbt test


⸻

7️⃣ Generate and Serve Documentation

dbt docs generate
dbt docs serve


⸻

🗂️ Model Architecture

We use a layered architecture:

Layer	Purpose
staging/	Source-aligned, cleaned models
intermediate/	Joins, aggregations, business logic
marts/	Analytics-ready, business-facing tables
seeds/	Static reference data (CSV)
snapshots/	Slowly Changing Dimensions (SCDs)


⸻

✅ Data Quality & Testing

We enforce data integrity with:
	•	Schema tests: not_null, unique, accepted_values
	•	Custom tests: Business logic via SQL
	•	Snapshots: Change tracking for audit/compliance

Example in schema.yml:

columns:
  - name: user_id
    tests:
      - not_null
      - unique


⸻

🧩 Contributing Guidelines
	1.	Create a new feature branch:

git checkout -b feature/your-feature


	2.	Make your changes.
	3.	Test locally:

dbt run
dbt test


	4.	Push to remote:

git push origin feature/your-feature


	5.	Open a Pull Request for review.

⸻

📝 Common dbt Commands

Command	Purpose
dbt debug	Validate connection and profile
dbt run	Build all models
dbt test	Run data tests
dbt seed	Load seed data
dbt snapshot	Run snapshot models
dbt docs generate	Build documentation site
dbt docs serve	Serve documentation locally


⸻

📜 Best Practices for AWS RDS PostgreSQL

✅ Use parameter groups to optimize performance.
✅ Enable automated backups.
✅ Use IAM or SSL for secure connections.
✅ Consider read replicas for scaling read-heavy workloads.
✅ Rotate database passwords securely.


⸻

📬 Contact

For questions or support:
	•	Alex Marx or slack:external_engineering_ai
	•	alex@rickhousemedia.com

