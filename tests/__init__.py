import asyncio
import sys

if sys.version_info < (3, 11):
    TimeoutErrorException = asyncio.TimeoutError
else:
    TimeoutErrorException = TimeoutError
