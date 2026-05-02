"""
CEA scraper routes — 3 endpoints.
"""

from fastapi import APIRouter

from app.helpers import ERROR_RESPONSES, handle_scraper
from app.schemas import APIResponse
from app.modules.cea.services import CEAScraperService

router = APIRouter(tags=["CEA Scrapers"])


@router.post(
    "/scrape/transmission-reports",
    response_model=APIResponse,
    summary="Scrape CEA Transmission Reports",
    description=(
        "Downloads RTM and TBCB Transmission Reports from CEA for the "
        "last 24 months.  This is a **long-running** scraper."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_transmission_reports():
    return handle_scraper(
        service_fn=CEAScraperService.run_transmission_reports,
        success_message="CEA Transmission Reports scraper completed successfully.",
        error_message="CEA Transmission Reports scraper failed.",
        error_code="TRANSMISSION_REPORTS_ERROR",
    )


@router.post(
    "/scrape/potential-re-zones",
    response_model=APIResponse,
    summary="Scrape 500 GW RE Integration PDFs",
    description=(
        "Downloads Transmission System PDFs for 500 GW Non-Fossil "
        "Capacity integration from the CEA website (Playwright-rendered)."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_potential_re_zones():
    return handle_scraper(
        service_fn=CEAScraperService.run_potential_re_zones,
        success_message="500 GW RE Integration scraper completed successfully.",
        error_message="500 GW RE Integration scraper failed.",
        error_code="POTENTIAL_RE_ZONES_ERROR",
    )


@router.post(
    "/scrape/nct-meetings",
    response_model=APIResponse,
    summary="Scrape NCT Meeting Minutes",
    description=(
        "Downloads National Committee on Transmission meeting minutes "
        "PDFs from the CEA website (Playwright-rendered)."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_nct_meetings():
    return handle_scraper(
        service_fn=CEAScraperService.run_nct_meetings,
        success_message="NCT Meeting Minutes scraper completed successfully.",
        error_message="NCT Meeting Minutes scraper failed.",
        error_code="NCT_MEETINGS_ERROR",
    )
