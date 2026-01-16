from abc import ABC, abstractmethod
from typing import Any


class Extractor(ABC):
    """Base contract for extractor pipelines."""

    name: str

    @abstractmethod
    def extract(self) -> Any:
        """Fetch raw data from the source."""

    @abstractmethod
    def transform(self, raw: Any) -> Any:
        """Normalize raw payload to a canonical structure."""

    @abstractmethod
    def load(self, data: Any) -> int:
        """Persist normalized data. Returns row count."""

    def run(self) -> int:
        raw = self.extract()
        data = self.transform(raw)
        return self.load(data)