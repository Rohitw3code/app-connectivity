"""
Service layer — CEA scraper modules.
"""

from app.helpers import execute_scraper


class CEAScraperService:
    """
    Orchestrates execution of all CEA scrapers/ modules.

    Every public ``run_*`` method maps 1-to-1 to a script file.
    """

    @staticmethod
    def run_transmission_reports() -> dict:
        """
        Source 06 — CEA Transmission Reports Scraper  (SYNCHRONOUS)
        ────────────────────────────────────────────────────────────
        Downloads RTM and TBCB Transmission Reports for the last
        24 months using CEA's AJAX endpoint.

        Target : https://cea.nic.in/transmission-reports/?lang=en
        Output : uploads/CTUIL-Transmission-Reports/{year}/{month}/
        """
        from app.scrapers import source_06_ctuil_transmission_reports_scraper as script

        return execute_scraper(
            script,
            label="CEA Transmission Reports",
            output_dir="uploads/CTUIL-Transmission-Reports",
        )

    @staticmethod
    def run_potential_re_zones() -> dict:
        """
        Source 10a — 500 GW RE Integration Scraper
        ───────────────────────────────────────────
        Downloads Transmission System PDFs for 500 GW Non-Fossil
        Capacity integration.  Uses Playwright.

        Target : https://cea.nic.in/psp___a_i/transmission-system-for-integration-of-over-500-gw-non-fossil-capacity-by-2030/?lang=en
        Output : uploads/CEA-500GW/
        """
        from app.scrapers import source_10a_cea_potential_rezones_scraper as script

        return execute_scraper(
            script,
            label="500 GW RE Integration",
            output_dir="uploads/CEA-500GW",
        )

    @staticmethod
    def run_nct_meetings() -> dict:
        """
        Source 10b — NCT Meeting Minutes Scraper
        ─────────────────────────────────────────
        Downloads National Committee on Transmission meeting
        minutes PDFs from CEA.  Uses Playwright.

        Target : https://cea.nic.in/comm-trans/national-committee-on-transmission/?lang=en
        Output : uploads/CEA-NCT-Minutes/
        """
        from app.scrapers import source_10b_cea_nct_meetings_scraper as script

        return execute_scraper(
            script,
            label="NCT Meeting Minutes",
            output_dir="uploads/CEA-NCT-Minutes",
        )
