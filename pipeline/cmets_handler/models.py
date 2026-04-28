"""
cmets_handler/models.py — Pydantic schemas for CMETS extraction
=================================================================
Edit this file to add/remove/rename output columns.
"""

from __future__ import annotations

from typing import Optional
from pydantic import BaseModel, ConfigDict, Field


class MappedRow(BaseModel):
    """One extracted data row from a CMETS PDF page."""

    model_config = ConfigDict(populate_by_name=True)

    project_location:   Optional[str] = Field(None, alias="Project Location")
    state:              Optional[str] = Field(None, alias="State")
    substaion:          Optional[str] = Field(None, alias="substaion")
    name_of_developers: Optional[str] = Field(None, alias="Name of the developers")
    type:               Optional[str] = Field(None, alias="type")
    gna_st2_id:         Optional[str] = Field(None, alias="GNA/ST II Application ID")
    lta_id:             Optional[str] = Field(None, alias="LTA Application ID")
    enhancement_id:     Optional[str] = Field(None, alias="Application ID under Enhancement 5.2 or revision")
    quantum_mw:         Optional[str] = Field(None, alias="Application Quantum (MW)(ST II)")
    nature_of_applicant:Optional[str] = Field(None, alias="Nature of Applicant")
    mode_criteria:      Optional[str] = Field(None, alias="Mode(Criteria for applying)")
    applied_start_date: Optional[str] = Field(None, alias="Applied Start of Connectivity sought by developer date")
    submission_date:    Optional[str] = Field(None, alias="Application/Submission Date")
    gna_op_date:        Optional[str] = Field(None, alias="GNA Operationalization Date")
    gna_op_yesno:       Optional[str] = Field(None, alias="GNA Operationalization (Yes/No)")
    app_status:         Optional[str] = Field(None, alias="Status of application(Withdrawn / granted. Revoked.)")
    psp_mwh:            Optional[str] = Field(None, alias="PSP MWh")
    psp_injection:      Optional[str] = Field(None, alias="PSP Injection (MW)")
    psp_drawl:          Optional[str] = Field(None, alias="PSP Drawl (MW)")


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


# Column order for cmets.xlsx
CMETS_COLUMNS = [
    "PDF", "Page Number",
    # ── Meeting-level metadata (same for all rows from same PDF) ──
    "CMETS GNA Approved", "CMETS LTA Approved",
    "CMETS GNA Meeting Date", "CMETS LTA Meeting Date",
    # ── Row-level extracted data ──
    "Project Location", "State", "substaion", "Name of the developers",
    "type",
    "GNA/ST II Application ID", "LTA Application ID",
    "Application ID under Enhancement 5.2 or revision",
    "Application Quantum (MW)(ST II)", "Nature of Applicant",
    "Mode(Criteria for applying)",
    "Applied Start of Connectivity sought by developer date"
    "( start date of connectivity as per the application)",
    "Application/Submission Date", "GNA Operationalization Date",
    "GNA Operationalization (Yes/No)",
    "Status of application(Withdrawn / granted. Revoked.)",
    "PSP MWh", "PSP Injection (MW)", "PSP Drawl (MW)",
]
