from kstreams import Stream

from .streams_roster import stream_roster

my_stream = Stream(
    "local--hello-world",
    func=stream_roster,
    config={
        "group_id": "example-group",
    },
)
