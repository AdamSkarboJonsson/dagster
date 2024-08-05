from dataclasses import dataclass
from typing import Callable, List, Optional

from dagster import AssetSpec, Definitions, multi_asset

from .def_factory import DefsFactory


@dataclass
class PythonFnDefs(DefsFactory):
    """Create definitions that are backed by a Python function. Meant to
    replace `PythonOperator` in your Airflow installation.
    """

    specs: List[AssetSpec]
    name: str
    python_fn: Optional[Callable] = None

    def build_defs(self) -> Definitions:
        @multi_asset(
            specs=self.specs,
            name=self.name,
        )
        def _multi_asset() -> None:
            if self.python_fn:
                self.python_fn()

        return Definitions(assets=[_multi_asset])
