"""
cmets_handler/models.py — Pydantic schemas for CMETS extraction
=================================================================
Edit column_registry.py to add/remove/rename output columns.
This file defines the Pydantic models that match the column registry.
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field

# Import canonical column list from the registry
from pipeline.cmets_handler.column_registry import CMETS_COLUMNS


class MappedRow(BaseModel):
    """One extracted data row from a CMETS PDF page.

    Fields map 1:1 to the extraction and derived columns in column_registry.py.
    """

    model_config = ConfigDict(populate_by_name=True)

    # ── Extraction columns (from PDF page text via LLM) ──────────────────────
    substation:          Optional[str] = Field(None, alias="Substation")
    project_location:    Optional[str] = Field(None, alias="Project Location")
    name_of_developers:  Optional[str] = Field(None, alias="Name of Developers")
    gna_st2_id:          Optional[str] = Field(None, alias="GNA/ST II Application ID")
    lta_id:              Optional[str] = Field(None, alias="LTA Application ID")
    enhancement_id:      Optional[str] = Field(None, alias="Application ID under Enhancement 5.2 or revision")
    quantum_mw:          Optional[str] = Field(None, alias="Application Quantum (MW)(ST II)")
    granted_quantum:     Optional[str] = Field(None, alias="Granted Quantum GNA/LTA(MW)")

    # ── Battery (BESS) columns ───────────────────────────────────────────────
    battery_mwh:         Optional[str] = Field(None, alias="Battery MWh")
    battery_injection:   Optional[str] = Field(None, alias="Battery Injection (MW)")
    battery_drawl:       Optional[str] = Field(None, alias="Battery Drawl (MW)")

    # ── PSP (Pump Storage) columns ───────────────────────────────────────────
    psp_mwh:             Optional[str] = Field(None, alias="PSP MWh")
    psp_injection:       Optional[str] = Field(None, alias="PSP Injection (MW)")
    psp_drawl:           Optional[str] = Field(None, alias="PSP Drawl (MW)")

    submission_date:     Optional[str] = Field(None, alias="Application/Submission Date")
    mode_criteria:       Optional[str] = Field(None, alias="Mode(Criteria for applying)")
    applied_start_date:  Optional[str] = Field(
        None,
        alias="Applied Start of Connectivity sought by developer date"
              "( start date of connectivity as per the application)",
    )
    additional_capacity_date: Optional[str] = Field(None, alias="Date from which additional capacity is to be added")
    nature_of_applicant: Optional[str] = Field(None, alias="Nature of Applicant")
    app_status:          Optional[str] = Field(None, alias="Status of application(Withdrawn / granted. Revoked.)")
    voltage_level:       Optional[str] = Field(None, alias="Voltage level")

    # ── Derived columns ──────────────────────────────────────────────────────
    state:               Optional[str] = Field(None, alias="State")
    type:                Optional[str] = Field(None, alias="Type")

    # ── Calculated columns ───────────────────────────────────────────────────
    gna_op_date:         Optional[str] = Field(None, alias="GNA Operationalization Date")
    gna_op_yesno:        Optional[str] = Field(None, alias="GNA Operationalization (Yes/No)")


class PageResult(BaseModel):
    """Extraction result for a single page."""
    page_number: int
    rows_found:  int
    rows:        list[MappedRow]


class PipelineResult(BaseModel):
    """Extraction result for a single PDF."""
    pdf_path:              str
    total_pages_extracted: int
    pages_passed_gate:     int
    pages_skipped:         int
    total_rows:            int
    results:               list[PageResult]
