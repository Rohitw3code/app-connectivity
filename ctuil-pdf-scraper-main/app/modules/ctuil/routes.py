"""
CTUIL scraper routes — 9 endpoints.
"""

from fastapi import APIRouter

from app.helpers import ERROR_RESPONSES, handle_scraper
from app.schemas import APIResponse
from app.modules.ctuil.services import CtuILScraperService

router = APIRouter(tags=["CTUIL Scrapers"])


@router.post(
    "/scrape/ists-consultation-meeting",
    response_model=APIResponse,
    summary="Scrape ISTS Consultation Meeting PDFs",
    description=(
        "Downloads **Agenda** and **Minutes** PDFs for all five regions "
        "(NR, WR, SR, ER, NER) from the CTUIL ISTS Consultation Meeting page."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_ists_consultation_meeting():
    return handle_scraper(
        service_fn=CtuILScraperService.run_ists_consultation_meeting,
        success_message="ISTS Consultation Meeting scraper completed successfully.",
        error_message="ISTS Consultation Meeting scraper failed.",
        error_code="ISTS_CONSULTATION_MEETING_ERROR",
    )


@router.post(
    "/scrape/ists-joint-coordination-meeting",
    response_model=APIResponse,
    summary="Scrape ISTS Joint Coordination Meeting PDFs",
    description=(
        "Downloads **Notice** and **Minutes** PDFs for all regions from "
        "the CTUIL ISTS Joint Coordination Meeting page."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_ists_joint_coordination_meeting():
    return handle_scraper(
        service_fn=CtuILScraperService.run_ists_joint_coordination_meeting,
        success_message="ISTS Joint Coordination Meeting scraper completed successfully.",
        error_message="ISTS Joint Coordination Meeting scraper failed.",
        error_code="ISTS_JOINT_COORDINATION_MEETING_ERROR",
    )


@router.post(
    "/scrape/regenerators",
    response_model=APIResponse,
    summary="Scrape RE Generators PDFs",
    description=(
        "Downloads effective-date-wise connectivity PDFs for RE Generators "
        "from the CTUIL regenerators page (Playwright-rendered)."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_regenerators():
    return handle_scraper(
        service_fn=CtuILScraperService.run_regenerators,
        success_message="RE Generators scraper completed successfully.",
        error_message="RE Generators scraper failed.",
        error_code="REGENERATORS_ERROR",
    )


@router.post(
    "/scrape/reallocation-meetings",
    response_model=APIResponse,
    summary="Scrape Reallocation Meeting PDFs",
    description=(
        "Downloads **Agenda** and **Minutes** PDFs for all regions from "
        "the CTUIL Reallocation Meetings page (Playwright-rendered)."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_reallocation_meetings():
    return handle_scraper(
        service_fn=CtuILScraperService.run_reallocation_meetings,
        success_message="Reallocation Meetings scraper completed successfully.",
        error_message="Reallocation Meetings scraper failed.",
        error_code="REALLOCATION_MEETINGS_ERROR",
    )


@router.post(
    "/scrape/bidding-calendar",
    response_model=APIResponse,
    summary="Scrape Bidding Calendar PDFs",
    description="Downloads Bidding Calendar PDFs from the CTUIL website.",
    responses=ERROR_RESPONSES,
)
def scrape_bidding_calendar():
    return handle_scraper(
        service_fn=CtuILScraperService.run_bidding_calendar,
        success_message="Bidding Calendar scraper completed successfully.",
        error_message="Bidding Calendar scraper failed.",
        error_code="BIDDING_CALENDAR_ERROR",
    )


@router.post(
    "/scrape/compliance-fc",
    response_model=APIResponse,
    summary="Scrape Compliance & FC PDFs",
    description=(
        "Downloads **Connectivity Grantee** PDFs from the CTUIL "
        "Compliance & FC page."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_compliance_fc():
    return handle_scraper(
        service_fn=CtuILScraperService.run_compliance_fc,
        success_message="Compliance & FC scraper completed successfully.",
        error_message="Compliance & FC scraper failed.",
        error_code="COMPLIANCE_FC_ERROR",
    )


@router.post(
    "/scrape/monitoring-connectivity",
    response_model=APIResponse,
    summary="Scrape Monitoring / Revocations PDFs",
    description="Downloads Monitoring and Revocation PDFs from the CTUIL page.",
    responses=ERROR_RESPONSES,
)
def scrape_monitoring_connectivity():
    return handle_scraper(
        service_fn=CtuILScraperService.run_monitoring_connectivity,
        success_message="Monitoring / Revocations scraper completed successfully.",
        error_message="Monitoring / Revocations scraper failed.",
        error_code="MONITORING_CONNECTIVITY_ERROR",
    )


@router.post(
    "/scrape/renewable-energy",
    response_model=APIResponse,
    summary="Scrape Renewable Energy PDFs",
    description=(
        "Downloads RE margin PDFs (Non-RE, RE Substations, Proposed RE) "
        "and Bays Allocation PDFs from the CTUIL renewable-energy page."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_renewable_energy():
    return handle_scraper(
        service_fn=CtuILScraperService.run_renewable_energy,
        success_message="Renewable Energy scraper completed successfully.",
        error_message="Renewable Energy scraper failed.",
        error_code="RENEWABLE_ENERGY_ERROR",
    )


@router.post(
    "/scrape/substation-bulk-consumers",
    response_model=APIResponse,
    summary="Scrape Substation Bulk Consumer PDFs",
    description=(
        "Downloads Bulk Consumer PDFs from the CTUIL Substation page "
        "(Playwright-rendered)."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_substation_bulk_consumers():
    return handle_scraper(
        service_fn=CtuILScraperService.run_substation_bulk_consumers,
        success_message="Substation Bulk Consumers scraper completed successfully.",
        error_message="Substation Bulk Consumers scraper failed.",
        error_code="SUBSTATION_BULK_CONSUMERS_ERROR",
    )


@router.post(
    "/scrape/gna-connectivity-fresh",
    response_model=APIResponse,
    summary="Scrape GNA Connectivity Fresh PDFs",
    description=(
        "Downloads **Connectivity Fresh** PDFs for the latest 6 months "
        "from the CTUIL GNA 2022 updates page."
    ),
    responses=ERROR_RESPONSES,
)
def scrape_gna_connectivity_fresh():
    return handle_scraper(
        service_fn=CtuILScraperService.run_gna_connectivity_fresh,
        success_message="GNA Connectivity Fresh scraper completed successfully.",
        error_message="GNA Connectivity Fresh scraper failed.",
        error_code="GNA_CONNECTIVITY_FRESH_ERROR",
    )
