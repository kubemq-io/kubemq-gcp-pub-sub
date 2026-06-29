"""Enable `python -m delivery.exactly_once` (spec S5.6 module run form).

Loads the sibling main.py by path with run_name="__main__" so the example runs
identically under both `uv run python -m delivery.exactly_once` and
`uv run python delivery/exactly_once/main.py`, without needing package
`__init__.py` files (delivery/ is a namespace package).
"""

import runpy
from pathlib import Path

runpy.run_path(str(Path(__file__).with_name("main.py")), run_name="__main__")
