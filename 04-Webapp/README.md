# 04-Webapp

Local-first scaffold for the Georgia statewide web app. This follows the useful parts of the Raptor setup without remote hosting dependencies:

- `frontend/`: Vite + React + TypeScript + Zustand + React Query
- `backend/`: FastAPI + SQLAlchemy with `text(...)` queries
- `docker/postgis/init/`: local PostGIS bootstrap scripts
- `docker-compose.yml`: one-command local stack for PostGIS, API, and web UI

## Local-first choices

- Database: local PostGIS
- Map provider: MapLibre instead of Google Maps
- Chat: intentionally omitted for now

Those seams are isolated so the app can stay local-first while the codebase evolves.

## Run With Docker

```bash
docker compose up --build
```

Services:

- Frontend: `http://localhost:5173`
- Backend: `http://localhost:8000`
- OpenAPI: `http://localhost:8000/docs`
- PostGIS: `localhost:5432`

## Run Without Docker

Backend:

```bash
cd backend
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
uvicorn app.main:app --reload
```

Frontend:

```bash
cd frontend
npm install
npm run dev
```

The local environment files are already scaffolded as:

- `backend/.env`
- `frontend/.env`

Tracked templates live in:

- `backend/.env.example`
- `frontend/.env.example`
