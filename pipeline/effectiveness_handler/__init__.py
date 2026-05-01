"""
pipeline.effectiveness_handler — Effectiveness PDF Extraction Handler
======================================================================
Public API:
    from pipeline.effectiveness_handler import run_effectiveness_extraction
    from pipeline.effectiveness_handler import compute_installed_capacity
    from pipeline.effectiveness_handler import update_gna_dates, update_additional_capacity_dates
"""
from pipeline.effectiveness_handler.runner import run_effectiveness_extraction
from pipeline.effectiveness_handler.capacity_calculator import compute_installed_capacity
from pipeline.effectiveness_handler.date_updater import update_gna_dates, update_additional_capacity_dates

__all__ = [
    "run_effectiveness_extraction",
    "compute_installed_capacity",
    "update_gna_dates",
    "update_additional_capacity_dates",
]
