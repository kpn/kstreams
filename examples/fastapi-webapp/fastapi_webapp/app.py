from fastapi import FastAPI
from starlette_prometheus import PrometheusMiddleware, metrics

from .resources import stream_engine
from .streams import consume
from .views import router

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    await stream_engine.start()


@app.on_event("shutdown")
async def shutdown_event():
    await stream_engine.stop()


stream_engine.add_stream(consume)
app.include_router(router)
app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
app.add_api_route("/metrics", metrics)  # type: ignore
