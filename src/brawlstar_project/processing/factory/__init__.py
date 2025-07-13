"""
Factory module for pipeline stages.

This module provides factory patterns for different pipeline stages
(ingestion, analysis, etc.) with a unified interface.
"""

from .base_factory import BaseFactory, BaseRunner, PipelineStage
from .processing_factory import ProcessingFactory
from .runner_factory import RunnerFactory

__all__ = [
    "BaseRunner",
    "BaseFactory",
    "PipelineStage",
    "ProcessingFactory",
    "RunnerFactory",
]
