import logging

from sqlalchemy import select

from kstreams import ConsumerRecord, Send, stream

from .models import User
from .resources import Session

logger = logging.getLogger(__name__)


@stream("local--hello-world", group_id="rqlite-group")
async def consume(cr: ConsumerRecord, send: Send) -> None:
    logger.info(f"showing bytes: {cr.value}")

    with Session() as session:
        statement = select(User).filter_by()

        # list of ``User`` objects
        user_obj = session.scalars(statement).all()
        print(user_obj)
