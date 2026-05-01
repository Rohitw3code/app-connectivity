"""
pipeline.bay_mapping_handler — CMETS × Bay Allocation Mapping Handler (Module 6)
==================================================================================
Looks up each CMETS developer+voltage combination in the Bay Allocation data
and enriches CMETS rows with the matched bay number and substation coordinates.

Public API:
    from pipeline.bay_mapping_handler import run_bay_mapping
"""
from pipeline.bay_mapping_handler.runner import run_bay_mapping

__all__ = ["run_bay_mapping"]
