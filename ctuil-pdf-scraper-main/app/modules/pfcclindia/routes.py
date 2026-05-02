"""
PFCCLINDIA scraper routes — 1 endpoint.
"""

from fastapi import APIRouter, Form

from app.helpers import ERROR_RESPONSES, handle_scraper
from app.schemas import APIResponse
from app.modules.pfcclindia.services import PfcclIndiaScraperService

router = APIRouter(tags=["PFCCL-INDIA Scrapers"])


@router.post(
    "/scrape/pfcclindia-tender",
    response_model=APIResponse,
    summary="Scrape PFCCLINDIA Tender PDFs",
    description=(
        "Searches the PFCCLINDIA tender listing for entries whose title contains "
        "the given **query** substring, then downloads all matched PDFs filtered by "
        "keyword: *Corrigendum, Extension, Successful, RFP, Postponement, Qualified, Amendment*. "
        "Files are saved to `uploads/PFCCL-INDIA-TENDER/<folder>/`."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_pfcclindia_tender(
    query: str = Form(..., description="Substring of the tender title to search for"),
):
    return handle_scraper(
        service_fn=lambda: PfcclIndiaScraperService.run_pfcclindia_tender(query=query),
        success_message="PFCCLINDIA Tender scraper completed successfully.",
        error_message="PFCCLINDIA Tender scraper failed.",
        error_code="PFCCLINDIA_TENDER_ERROR",
    )
