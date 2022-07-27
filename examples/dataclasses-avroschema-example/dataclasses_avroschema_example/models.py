from dataclasses import dataclass
from dataclasses_avroschema import AvroModel

import typing


@dataclass
class User(AvroModel):
    "An User"
    name: str
    age: int
    pets: typing.List[str]
    accounts: typing.Dict[str, int]
    country: str = "Argentina"
    address: str = None

    class Meta:
        namespace = "User.v1"
        aliases = [
            "user-v1",
            "super user",
        ]


@dataclass
class Address(AvroModel):
    "An Address"
    street: str
    street_number: int

    class Meta:
        namespace = "Address.v1"
        aliases = [
            "address-v1",
        ]
