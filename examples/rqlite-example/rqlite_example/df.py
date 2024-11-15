from datafusion import SessionContext

ctx = SessionContext()
import random


df = ctx.from_pydict(
    {
        "nrs": [1, 2, 3, 4, 5],
        "names": ["python", "ruby", "java", "haskell", "go"],
        "random": random.sample(range(1000), 5),
        "groups": ["A", "A", "B", "C", "B"],
    }
)
