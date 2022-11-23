"""Remove this file when python3.8 support is dropped."""
import sys

if sys.version_info < (3, 9):
    from typing_extensions import Annotated as Annotated  # noqa: F401
else:
    from typing import Annotated as Annotated  # noqa: F401
