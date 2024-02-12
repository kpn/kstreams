from dataclasses_avroschema import AvroModel

from kstreams import ConsumerRecord, middleware


class AvroDeserializerMiddleware(middleware.BaseMiddleware):
    def __init__(self, *, model: AvroModel, **kwargs) -> None:
        super().__init__(**kwargs)
        self.model = model

    async def __call__(self, cr: ConsumerRecord):
        """
        Deserialize a payload to an AvroModel
        """
        if cr.value is not None:
            data = self.model.deserialize(cr.value)
            cr.value = data
        return await self.next_call(cr)
