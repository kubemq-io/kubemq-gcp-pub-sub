"""Enable `python -m advanced.schema_protobuf` (spec S5.6 module run form).

Loads the sibling main.py by path with run_name="__main__" so the example runs
identically under both `uv run python -m advanced.schema_protobuf` and
`uv run python advanced/schema_protobuf/main.py`, without needing package
`__init__.py` files (advanced/ is a namespace package).
"""

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).with_name("main.py")), run_name="__main__")
