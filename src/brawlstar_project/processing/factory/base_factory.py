"""
Unified factory system for all pipeline stages.

This module provides a common factory pattern that can be extended
for different pipeline stages (ingestion, analysis, etc.).
"""

from abc import ABC, abstractmethod
from enum import Enum
from typing import Dict, Type


class PipelineStage(Enum):
    """Enumeration of pipeline stages."""
    INGESTION = "ingestion"
    ANALYSIS = "analysis"
    PROCESSING = "processing"


class BaseRunner(ABC):
    """Abstract base class for all pipeline runners."""

    @abstractmethod
    def run(self, **kwargs) -> dict:
        """
        Run the pipeline stage.

        Returns:
            dict: statistics or results
        """
        pass


class BaseFactory:
    """Base factory class that can be extended for different stages."""

    def __init__(self):
        self._registry: Dict[str, Type[BaseRunner]] = {}

    def register(self, mode: str, runner_class: Type[BaseRunner]) -> None:
        """Register a runner class for a specific mode."""
        self._registry[mode] = runner_class

    def get_runner(self, mode: str) -> BaseRunner:
        """
        Get a runner instance for the given mode.

        Args:
            mode: Mode string

        Returns:
            BaseRunner instance

        Raises:
            ValueError: if mode is unknown
        """
        try:
            runner_cls = self._registry[mode]
            return runner_cls()
        except KeyError:
            raise ValueError(f"Unknown mode: {mode}")

    def list_modes(self) -> list:
        """List all available modes."""
        return list(self._registry.keys()) 