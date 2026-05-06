"""
Download catalog for scraper modules merged from ``ctuil-pdf-scraper-main``.

The ``output_dir`` values intentionally preserve the original scraper folder
layout.  The downloader wrapper runs the scrapers from the configured output
root, so these relative paths land under ``output/source_output/...`` by default.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


LimitStrategy = Literal[
    "collect_records_by_doc_type",
    "collect_mapping_values",
    "plan_sequence",
    "plan_urls",
    "apply_incremental_urls",
    "download_by_category",
    "download_counter",
]


@dataclass(frozen=True)
class ScraperSpec:
    key: str
    label: str
    module: str
    output_dir: str
    handler: str
    limit_strategy: LimitStrategy
    output_attr: str | None = None
    requires_query: bool = False


SCRAPER_SPECS: tuple[ScraperSpec, ...] = (
    ScraperSpec(
        key="cmets",
        label="ISTS Consultation Meeting",
        module="source_01_ctuil_ists_consultation_meeting_scraper",
        output_dir="source_output/CTUIL-ISTS-CMETS",
        handler="cmets",
        limit_strategy="collect_records_by_doc_type",
        output_attr="OUTPUT_DIR",
    ),
    ScraperSpec(
        key="jcc",
        label="ISTS Joint Coordination Meeting",
        module="source_02_ctuil_ists_joint_coordination_meeting_scraper",
        output_dir="source_output/CTUIL-ISTS-JCC",
        handler="jcc",
        limit_strategy="collect_mapping_values",
        output_attr="OUTPUT_DIR",
    ),
    ScraperSpec(
        key="effectiveness",
        label="RE Generators",
        module="source_03_ctuil_regenerators_scraper",
        output_dir="source_output/CTUIL-Regenerators-Effective-Date-wise",
        handler="effectiveness",
        limit_strategy="plan_sequence",
        output_attr="TARGET_DIR",
    ),
    ScraperSpec(
        key="reallocation_meetings",
        label="Reallocation Meetings",
        module="source_04_ctuil_reallocation_meetings_scraper",
        output_dir="source_output/CTUIL-Reallocation-Meetings",
        handler="reallocation_meetings",
        limit_strategy="apply_incremental_urls",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="bidding_calendar",
        label="Bidding Calendar",
        module="source_05_ctuil_bidding_calender_scraper",
        output_dir="source_output/CTUIL-Bidding-Calendar",
        handler="bidding_calendar",
        limit_strategy="plan_urls",
        output_attr="DOWNLOAD_DIR",
    ),
    ScraperSpec(
        key="transmission_reports",
        label="CEA Transmission Reports",
        module="source_06_ctuil_transmission_reports_scraper",
        output_dir="source_output/CTUIL-Transmission-Reports",
        handler="transmission_reports",
        limit_strategy="download_by_category",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="compliance_fc",
        label="Compliance & FC",
        module="source_07_ctuil_compliance_fc_scraper",
        output_dir="source_output/CTUIL-Compliance-PDFs",
        handler="compliance_fc",
        limit_strategy="plan_urls",
        output_attr="DOWNLOAD_DIR",
    ),
    ScraperSpec(
        key="monitoring_connectivity",
        label="Monitoring / Revocations",
        module="source_08_ctuil_monitoring_connectivity_scraper",
        output_dir="source_output/CTUIL-Revocations-PDFs",
        handler="monitoring_connectivity",
        limit_strategy="plan_urls",
    ),
    ScraperSpec(
        key="bayallocation",
        label="Renewable Energy",
        module="source_09_ctuil_renewable_energy_scraper",
        output_dir="source_output/CTUIL-Renewable-Energy",
        handler="bayallocation",
        limit_strategy="plan_urls",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="potential_re_zones",
        label="500 GW RE Integration",
        module="source_10a_cea_potential_rezones_scraper",
        output_dir="source_output/CEA-500GW",
        handler="potential_re_zones",
        limit_strategy="plan_urls",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="nct_meetings",
        label="NCT Meeting Minutes",
        module="source_10b_cea_nct_meetings_scraper",
        output_dir="source_output/CEA-NCT-Minutes",
        handler="nct_meetings",
        limit_strategy="plan_sequence",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="pfcclindia_tender",
        label="PFCCLINDIA Tender",
        module="source_10c_pfcclindia_tender_scraper",
        output_dir="source_output/PFCCL-INDIA-TENDER",
        handler="pfcclindia_tender",
        limit_strategy="download_counter",
        requires_query=True,
    ),
    ScraperSpec(
        key="substation_bulk_consumers",
        label="Substation Bulk Consumers",
        module="source_11_ctuil_substation_bulk_consumers_scraper",
        output_dir="source_output/CTUIL-Bulk-Consumers",
        handler="substation_bulk_consumers",
        limit_strategy="plan_urls",
        output_attr="BASE_DIR",
    ),
    ScraperSpec(
        key="gna_connectivity_fresh",
        label="GNA Connectivity Fresh",
        module="source_12_ctuil_gna_connectivity_fresh_scraper",
        output_dir="source_output/CTUIL-GNA-Connectivity-Fresh",
        handler="gna_connectivity_fresh",
        limit_strategy="plan_sequence",
        output_attr="DOWNLOAD_DIR",
    ),
)


SPECS_BY_KEY = {spec.key: spec for spec in SCRAPER_SPECS}
