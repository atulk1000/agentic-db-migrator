# agentic-db-migrator
AI-driven database migration orchestrator that reads live schema metadata, generates an execution plan (ordering, batching, partitioning, validations), and runs migrations with guardrails and verification.with safety guardrails and verification
# 🚀 Agentic Migration Orchestrator

An **AI-driven database migration orchestrator** that inspects live schema metadata, generates an execution plan (ordering, partitioning, batching, validations), and executes migrations deterministically with safety guardrails and verification.

Instead of hardcoding migration logic, this framework allows an intelligent planner to decide *how* the migration should run — while enforcing strict policy controls and resumable execution.

---

## 🧠 What Makes It Agentic?

The system reads source database metadata and allows an agent to:

- Decide migration ordering across schemas and tables  
- Select partitioning and batching strategies per table  
- Detect special cases (large tables, partitions, geometry columns, materialized views)  
- Choose validation checks to run post-migration  
- Flag risk conditions before execution  

The generated execution plan is structured, validated against policy constraints, and then executed deterministically.

> The agent generates the migration execution plan.  
> The engine enforces policy and executes it safely.

---

## 🏗 Architecture

### 1️⃣ Discover  
Inspects the source database and builds a structured `MigrationManifest` including:

- Schemas and tables  
- Estimated row counts  
- Partition metadata  
- Geometry columns  
- Materialized views  

### 2️⃣ Plan  
An agent generates a `MigrationPlan` that defines:

- Table migration order  
- Parallelism and partitioning strategy  
- Batch sizes  
- Validation rules  
- Object dependency sequencing  

### 3️⃣ Execute  
The execution engine:

- Applies migration steps deterministically  
- Supports checkpoint/resume  
- Handles partitioned tables and geometry columns  
- Rebuilds dependent objects  

### 4️⃣ Verify  
Post-migration validation produces a structured report including:

- Rowcount parity checks  
- Optional sampling/hash checks  
- Success/failure summary  

---

## 🛡 Safety Design

This system is **AI-assisted, not AI-autonomous**.

- All plans are validated against an allowlisted policy  
- Destructive operations require explicit flags  
- Partition and concurrency caps are enforced  
- The executor never runs arbitrary SQL from model text  
- Structured schemas (Pydantic models) define all plan contracts  

This ensures intelligent planning without unsafe execution.

---

## ✨ Core Features

- 🔍 Automatic schema discovery  
- 🧠 AI-generated execution planning (model-agnostic)  
- 🛡 Policy enforcement and guardrails  
- ⚙ Spark + JDBC deterministic execution  
- ♻ Checkpoint and resume support  
- 📊 Post-migration verification report  
- 🧩 Extensible step-handler architecture  

---

## 🔌 Model-Agnostic Design

The planner is implemented via an adapter interface:

- `heuristic` planner (default, deterministic)  
- `openai` planner (optional adapter)  
- `ollama` planner (optional adapter)  

Execution logic remains unchanged regardless of planner implementation.

---

## 🚀 Quickstart (Local Demo)

### 1. Start local Postgres instances

```bash
docker compose up -d

amo discover --config config.yaml --out manifest.json

2) Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

3) Install the package (editable)
pip install -e .

4) Copy config templates
cp .env.example .env
cp config.example.yaml config.yaml

5) Discover schema metadata (manifest)
amo discover --config config.yaml --out manifest.json

6) Generate an execution plan
amo plan --manifest manifest.json --planner heuristic --out plan.json

7) Run the migration
amo run --config config.yaml --plan plan.json --state state.json

8) Verify the migration
amo verify --config config.yaml --plan plan.json --out report.json
```

---
## 📦 Project Structure

```text
src/amo/
  core/
    manifest_builder.py
    heuristic_planner.py
    executor.py
    verifier.py
    policy.py
  migration/
    transfer.py
    introspect.py
    geometry.py
    matviews.py
    indexes.py
    keys.py
    grants.py
  planners/
    openai_stub.py
    ollama_stub.py
```
---
## 🧭 Roadmap

Full LLM-based planning adapter

Intelligent risk scoring for large environments

Cost-aware execution strategy optimization

Cross-database engine support

Advanced validation (distribution drift, checksum comparison)

---
## 🎯 Motivation
Traditional migration scripts are:

Hardcoded

Environment-specific

Fragile under scale

Opaque in decision-making

This project explores a safer pattern:

Let intelligence decide the plan.
Let deterministic systems execute it safely.
Let verification prove correctness
