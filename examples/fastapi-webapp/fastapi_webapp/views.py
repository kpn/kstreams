from fastapi import APIRouter
from fastapi.responses import JSONResponse

from .resources import stream_engine

router = APIRouter()


@router.get("/events")
async def produce_event():
    """Send an event to the cluster.

    This should be a POST, but like this it can be
    easily executed in the browser.
    """
    payload = '{"message": "hello world!"}'

    metadata = await stream_engine.send(
        "local--kstream",
        value=payload.encode(),
    )

    msg = {
        "topic": metadata.topic,
        "partition": metadata.partition,
        "offset": metadata.offset,
    }
    return JSONResponse(content=msg)
