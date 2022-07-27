from fastapi import FastAPI
from starlette.responses import Response
from starlette_prometheus import PrometheusMiddleware, metrics

from .streaming.streams import stream_engine


def create_app():
    app = FastAPI()

    add_endpoints(app)
    _setup_prometheus(app)

    @app.on_event("startup")
    async def startup_event():
        await stream_engine.start()

    @app.on_event("shutdown")
    async def shutdown_event():
        await stream_engine.stop()

    return app


def add_endpoints(app):
    @app.get("/events")
    async def post_produce_event() -> None:
        payload = '{"message": "hello world!"}'

        metadata = await stream_engine.send(
            "local--kstream",
            value=payload.encode(),
        )
        msg = (
            f"Produced event on topic: {metadata.topic}, "
            f"part: {metadata.partition}, offset: {metadata.offset}"
        )
        return Response(msg)


def _setup_prometheus(app: FastAPI) -> None:
    app.add_middleware(PrometheusMiddleware, filter_unhandled_paths=True)
    app.add_api_route("/metrics", metrics)


application = create_app()
