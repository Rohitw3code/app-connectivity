"""
pipeline/downloader/other_scrapers.py — Scrapers Without Extraction Handlers
===============================================================================
These scrapers are preserved from the original ctuil-pdf-scraper but have
NO extraction pipeline handlers. They only download PDFs.

Kept untouched as requested:
  - source_04: Reallocation Meetings
  - source_05: Bidding Calendar
  - source_06: CEA Transmission Reports
  - source_07: Compliance & FC
  - source_08: Monitoring / Revocations
  - source_10a: CEA 500GW RE Integration
  - source_10b: CEA NCT Meetings
  - source_10c: PFCCLINDIA Tender
  - source_11: Substation Bulk Consumers
  - source_12: GNA Connectivity Fresh

To run any of these, import the original scraper script module and call main().
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Import paths for original scraper scripts (kept in ctuil-pdf-scraper-main for now,
# but the actual scraper code is preserved in the scrapers/ subfolder below)

_SCRAPER_CATALOG = {
    "reallocation": {
        "label": "Reallocation Meetings",
        "source_url": "https://www.ctuil.in/reallocation_meetings",
        "output_dir": "uploads/CTUIL-Reallocation-Meetings",
    },
    "bidding": {
        "label": "Bidding Calendar",
        "source_url": "https://www.ctuil.in/bidding-calendar",
        "output_dir": "uploads/CTUIL-Bidding-Calendar",
    },
    "compliance": {
        "label": "Compliance & FC",
        "source_url": "https://ctuil.in/complianceandfc",
        "output_dir": "uploads/CTUIL-Compliance-PDFs",
    },
    "monitoring": {
        "label": "Monitoring / Revocations",
        "source_url": "https://www.ctuil.in/revocations",
        "output_dir": "uploads/CTUIL-Revocations-PDFs",
    },
    "transmission": {
        "label": "CEA Transmission Reports",
        "source_url": "https://cea.nic.in/transmission-reports/?lang=en",
        "output_dir": "uploads/CTUIL-Transmission-Reports",
    },
    "bulk_consumers": {
        "label": "Substation Bulk Consumers",
        "source_url": "https://ctuil.in/substation-bulk-consumers",
        "output_dir": "uploads/CTUIL-Bulk-Consumers",
    },
    "gna_fresh": {
        "label": "GNA Connectivity Fresh",
        "source_url": "https://www.ctuil.in/gna2022updates",
        "output_dir": "uploads/CTUIL-GNA-Connectivity-Fresh",
    },
    "cea_500gw": {
        "label": "500 GW RE Integration",
        "source_url": "https://cea.nic.in/psp___a_i/transmission-system-for-integration-of-over-500-gw-non-fossil-capacity-by-2030/?lang=en",
        "output_dir": "uploads/CEA-500GW",
    },
    "cea_nct": {
        "label": "NCT Meeting Minutes",
        "source_url": "https://cea.nic.in/comm-trans/national-committee-on-transmission/?lang=en",
        "output_dir": "uploads/CEA-NCT-Minutes",
    },
    "pfcclindia": {
        "label": "PFCCLINDIA Tender",
        "source_url": "https://www.pfcclindia.com/tender.php?AM2",
        "output_dir": "uploads/PFCCL-INDIA-TENDER",
    },
}


def list_other_scrapers() -> list[dict]:
    """Return metadata for all scrapers that have no extraction handler."""
    return [
        {"key": k, **v}
        for k, v in _SCRAPER_CATALOG.items()
    ]
