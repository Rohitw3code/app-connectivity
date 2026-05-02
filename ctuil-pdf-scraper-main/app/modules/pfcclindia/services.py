"""
Service layer — PFCCLINDIA scraper module.
"""

import logging
import time
from pathlib import Path

logger = logging.getLogger(__name__)


class PfcclIndiaScraperService:
    """
    Orchestrates execution of the PFCCLINDIA tender scraper.
    """

    @staticmethod
    def run_pfcclindia_tender(query: str) -> dict:
        """
        Source 10c — PFCCLINDIA Tender Scraper
        ────────────────────────────────────────
        Searches the PFCCLINDIA tender page for entries whose title contains
        the given query substring, then downloads all matched PDFs
        (filtered by keyword: Corrigendum, Extension, Successful, RFP,
        Postponement, Qualified, Amendment).

        Target : https://www.pfcclindia.com/tender.php?AM2
        Output : uploads/PFCCL-INDIA-TENDER/<folder_derived_from_query>/
        """
        from app.scrapers import source_10c_pfcclindia_tender_scraper as script

        label = "PFCCLINDIA Tender"
        logger.info("[START] %s  (query=%r)", label, query)
        start = time.time()

        output_dir = "uploads/PFCCL-INDIA-TENDER"
        out_path = Path(output_dir)
        out_path.mkdir(parents=True, exist_ok=True)

        script.run(user_input=query, output_dir=out_path)

        elapsed = round(time.time() - start, 2)
        logger.info("[DONE]  %s  completed in %ss", label, elapsed)

        folder_name = script.make_folder_name(query)
        return {
            "script": "source_10c_pfcclindia_tender_scraper",
            "query": query,
            "execution_time_seconds": elapsed,
            "output_dir": str(out_path / folder_name),
        }
