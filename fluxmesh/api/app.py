from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse

import fluxmesh.tasks  # Register builtin tasks
from fluxmesh.api.routes import get_broker, router


@asynccontextmanager
async def lifespan(app: FastAPI):
    broker = await get_broker()
    yield
    await broker.close()


def create_app() -> FastAPI:
    app = FastAPI(
        title="FluxMesh Orchestration API",
        description="Distributed job orchestration platform with intelligent scheduling, retries, priorities, and fault-tolerant execution.",
        version="0.1.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(router)

    @app.get("/", include_in_schema=False)
    async def root():
        return RedirectResponse(url="/dashboard")

    return app


app = create_app()
