import logging
import uvicorn
from fastapi import FastAPI

from app.catalog import SCRAPER_CATALOG
from app.schemas import APIResponse
from app.modules.ctuil.routes import router as ctuil_router
from app.modules.cea.routes import router as cea_router
from app.modules.pfcclindia.routes import router as pfcclindia_router

# ==== Logging ====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# ==== FastAPI App ====
app = FastAPI(
    title="CTUIL / CEA / PFCCLINDIA Scraper API",
    description=(
        "REST API wrapping all PDF scraper modules. "
    ),
    version="2.0.0",
)


# ==== Health & API Info ====
@app.get("/", tags=["Health"], response_model=APIResponse, summary="Health Check")
def health():
    """Server health-check and status endpoint."""
    return APIResponse.success(
        message="Scraper API is running. Visit /docs for documentation.",
        data={"available_scrapers": len(SCRAPER_CATALOG)},
    )


@app.get(
    "/api/v1/scrapers",
    tags=["API Info"],
    response_model=APIResponse,
    summary="List all available scrapers",
    description="Returns metadata for every registered scraper endpoint.",
)
def list_scrapers():
    return APIResponse.success(
        message=f"{len(SCRAPER_CATALOG)} scrapers available.",
        data={"scrapers": SCRAPER_CATALOG},
    )


# ==== Register routers ====
app.include_router(ctuil_router, prefix="/api/v1")
app.include_router(cea_router, prefix="/api/v1")
app.include_router(pfcclindia_router, prefix="/api/v1")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
