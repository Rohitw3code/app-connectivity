"""
Catalog - metadata for all registered scrapers.

Used by the API Info endpoint and the health check.
"""

SCRAPER_CATALOG = [
    {
        "endpoint": "/api/v1/scrape/ists-consultation-meeting",
        "label": "ISTS Consultation Meeting",
        "source": "CTUIL",
        "page_url": "https://ctuil.in/ists-consultation-meeting",
        "output_dir": "uploads/CTUIL-ISTS-CMETS",
    },
    {
        "endpoint": "/api/v1/scrape/ists-joint-coordination-meeting",
        "label": "ISTS Joint Coordination Meeting",
        "source": "CTUIL",
        "page_url": "https://ctuil.in/ists-joint-coordination-meeting",
        "output_dir": "uploads/CTUIL-ISTS-JCC",
    },
    {
        "endpoint": "/api/v1/scrape/regenerators",
        "label": "RE Generators",
        "source": "CTUIL",
        "page_url": "https://ctuil.in/regenerators",
        "output_dir": "uploads/CTUIL-Regenerators-Effective-Date-wise",
    },
    {
        "endpoint": "/api/v1/scrape/reallocation-meetings",
        "label": "Reallocation Meetings",
        "source": "CTUIL",
        "page_url": "https://www.ctuil.in/reallocation_meetings",
        "output_dir": "uploads/CTUIL-Reallocation-Meetings",
    },
    {
        "endpoint": "/api/v1/scrape/bidding-calendar",
        "label": "Bidding Calendar",
        "source": "CTUIL",
        "page_url": "https://www.ctuil.in/bidding-calendar",
        "output_dir": "uploads/CTUIL-Bidding-Calendar",
    },
    {
        "endpoint": "/api/v1/scrape/compliance-fc",
        "label": "Compliance & FC",
        "source": "CTUIL",
        "page_url": "https://ctuil.in/complianceandfc",
        "output_dir": "uploads/CTUIL-Compliance-PDFs",
    },
    {
        "endpoint": "/api/v1/scrape/monitoring-connectivity",
        "label": "Monitoring / Revocations",
        "source": "CTUIL",
        "page_url": "https://www.ctuil.in/revocations",
        "output_dir": "uploads/CTUIL-Revocations-PDFs",
    },
    {
        "endpoint": "/api/v1/scrape/renewable-energy",
        "label": "Renewable Energy",
        "source": "CTUIL",
        "page_url": "https://www.ctuil.in/renewable-energy",
        "output_dir": "uploads/CTUIL-Renewable-Energy",
    },
    {
        "endpoint": "/api/v1/scrape/substation-bulk-consumers",
        "label": "Substation Bulk Consumers",
        "source": "CTUIL",
        "page_url": "https://ctuil.in/substation-bulk-consumers",
        "output_dir": "uploads/CTUIL-Bulk-Consumers",
    },
    {
        "endpoint": "/api/v1/scrape/transmission-reports",
        "label": "CEA Transmission Reports",
        "source": "CEA",
        "page_url": "https://cea.nic.in/transmission-reports/?lang=en",
        "output_dir": "uploads/CTUIL-Transmission-Reports",
    },
    {
        "endpoint": "/api/v1/scrape/potential-re-zones",
        "label": "500 GW RE Integration",
        "source": "CEA",
        "page_url": "https://cea.nic.in/psp___a_i/transmission-system-for-integration-of-over-500-gw-non-fossil-capacity-by-2030/?lang=en",
        "output_dir": "uploads/CEA-500GW",
    },
    {
        "endpoint": "/api/v1/scrape/nct-meetings",
        "label": "NCT Meeting Minutes",
        "source": "CEA",
        "page_url": "https://cea.nic.in/comm-trans/national-committee-on-transmission/?lang=en",
        "output_dir": "uploads/CEA-NCT-Minutes",
    },
    {
        "endpoint": "/api/v1/scrape/pfcclindia-tender",
        "label": "PFCCLINDIA Tender",
        "source": "PFCCLINDIA",
        "page_url": "https://www.pfcclindia.com/tender.php?AM2",
        "output_dir": "uploads/PFCCL-INDIA-TENDER",
        "input_required": {"query": "Substring of the tender title to search for"},
    },
    {
        "endpoint": "/api/v1/scrape/gna-connectivity-fresh",
        "label": "GNA Connectivity Fresh",
        "source": "CTUIL",
        "page_url": "https://www.ctuil.in/gna2022updates",
        "output_dir": "uploads/CTUIL-GNA-Connectivity-Fresh",
    },
]
