"""
Factory module for pipeline stages.

This module provides factory patterns for different pipeline stages
(ingestion, analysis, etc.) with a unified interface.
"""

from .analysis_factory import AnalysisFactory, AnalysisRunner
from .base_factory import BaseFactory, BaseRunner, PipelineStage

__all__ = [
    "BaseRunner",
    "BaseFactory", 
    "PipelineStage",
    "AnalysisFactory",
    "AnalysisRunner"
] 