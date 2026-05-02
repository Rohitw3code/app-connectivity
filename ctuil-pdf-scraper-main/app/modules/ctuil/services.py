"""
Service layer — CTUIL scraper modules.
"""

from app.helpers import execute_scraper


class CtuILScraperService:
    """
    Orchestrates execution of all CTUIL scrapers/ modules.

    Every public ``run_*`` method maps 1-to-1 to a script file.
    """

    @staticmethod
    def run_ists_consultation_meeting() -> dict:
        """
        Source 01 — ISTS Consultation Meeting Scraper
        ──────────────────────────────────────────────
        Downloads **Agenda** and **Minutes** PDFs for all five regions
        (NR, WR, SR, ER, NER) across all paginated pages.

        Target : https://ctuil.in/ists-consultation-meeting
        Output : uploads/CTUIL-ISTS-CMETS/{Region}/{Agenda|Minutes}/
        """
        from app.scrapers import source_01_ctuil_ists_consultation_meeting_scraper as script

        return execute_scraper(
            script,
            label="ISTS Consultation Meeting",
            output_dir="uploads/CTUIL-ISTS-CMETS",
        )

    @staticmethod
    def run_ists_joint_coordination_meeting() -> dict:
        """
        Source 02 — ISTS Joint Coordination Meeting Scraper
        ───────────────────────────────────────────────────
        Downloads **Notice** and **Minutes** PDFs for all regions.

        Target : https://ctuil.in/ists-joint-coordination-meeting
        Output : uploads/CTUIL-ISTS-JCC/{Region}/{Notice|Minutes}/
        """
        from app.scrapers import source_02_ctuil_ists_joint_coordination_meeting_scraper as script

        return execute_scraper(
            script,
            label="ISTS Joint Coordination Meeting",
            output_dir="uploads/CTUIL-ISTS-JCC",
        )

    @staticmethod
    def run_regenerators() -> dict:
        """
        Source 03 — RE Generators Scraper
        ─────────────────────────────────
        Downloads **effective-date-wise connectivity** PDFs.
        Uses Playwright to render the JS-heavy page.

        Target : https://ctuil.in/regenerators
        Output : uploads/CTUIL-Regenerators-Effective-Date-wise/
        """
        from app.scrapers import source_03_ctuil_regenerators_scraper as script

        return execute_scraper(
            script,
            label="RE Generators",
            output_dir="uploads/CTUIL-Regenerators-Effective-Date-wise",
        )

    @staticmethod
    def run_reallocation_meetings() -> dict:
        """
        Source 04 — Reallocation Meetings Scraper
        ──────────────────────────────────────────
        Downloads **Agenda** and **Minutes** PDFs for all regions.
        Uses Playwright to navigate region tabs.

        Target : https://www.ctuil.in/reallocation_meetings
        Output : uploads/CTUIL-Reallocation-Meetings/{Region}/{agenda|minutes}/
        """
        from app.scrapers import source_04_ctuil_reallocation_meetings_scraper as script

        return execute_scraper(
            script,
            label="Reallocation Meetings",
            output_dir="uploads/CTUIL-Reallocation-Meetings",
        )

    @staticmethod
    def run_bidding_calendar() -> dict:
        """
        Source 05 — Bidding Calendar Scraper
        ────────────────────────────────────
        Downloads Bidding Calendar PDFs from CTUIL.

        Target : https://www.ctuil.in/bidding-calendar
        Output : uploads/CTUIL-Bidding-Calendar/
        """
        from app.scrapers import source_05_ctuil_bidding_calender_scraper as script

        return execute_scraper(
            script,
            label="Bidding Calendar",
            output_dir="uploads/CTUIL-Bidding-Calendar",
        )

    @staticmethod
    def run_compliance_fc() -> dict:
        """
        Source 07 — Compliance & FC Scraper
        ───────────────────────────────────
        Downloads **Connectivity Grantee** PDFs.

        Target : https://ctuil.in/complianceandfc
        Output : uploads/CTUIL-Compliance-PDFs/
        """
        from app.scrapers import source_07_ctuil_compliance_fc_scraper as script

        return execute_scraper(
            script,
            label="Compliance & FC",
            output_dir="uploads/CTUIL-Compliance-PDFs",
        )

    @staticmethod
    def run_monitoring_connectivity() -> dict:
        """
        Source 08 — Monitoring / Revocations Scraper
        ─────────────────────────────────────────────
        Downloads Monitoring and Revocation PDFs.

        Target : https://www.ctuil.in/revocations
        Output : uploads/CTUIL-Revocations-PDFs/
        """
        from app.scrapers import source_08_ctuil_monitoring_connectivity_scraper as script

        return execute_scraper(
            script,
            label="Monitoring / Revocations",
            output_dir="uploads/CTUIL-Revocations-PDFs",
        )

    @staticmethod
    def run_renewable_energy() -> dict:
        """
        Source 09 — Renewable Energy Scraper
        ────────────────────────────────────
        Downloads RE margin PDFs (Non-RE, RE Substations, Proposed RE)
        and Bays Allocation PDFs.  Uses Playwright.

        Target : https://www.ctuil.in/renewable-energy
        Output : uploads/CTUIL-Renewable-Energy/{bays_allocation|margin}/
        """
        from app.scrapers import source_09_ctuil_renewable_energy_scraper as script

        return execute_scraper(
            script,
            label="Renewable Energy",
            output_dir="uploads/CTUIL-Renewable-Energy",
        )

    @staticmethod
    def run_substation_bulk_consumers() -> dict:
        """
        Source 11 — Substation Bulk Consumers Scraper
        ──────────────────────────────────────────────
        Downloads Bulk Consumer PDFs.  Uses Playwright.

        Target : https://ctuil.in/substation-bulk-consumers
        Output : uploads/CTUIL-Bulk-Consumers/
        """
        from app.scrapers import source_11_ctuil_substation_bulk_consumers_scraper as script

        return execute_scraper(
            script,
            label="Substation Bulk Consumers",
            output_dir="uploads/CTUIL-Bulk-Consumers",
        )

    @staticmethod
    def run_gna_connectivity_fresh() -> dict:
        """
        Source 12 — GNA Connectivity Fresh Scraper
        ───────────────────────────────────────────
        Downloads **Connectivity Fresh** PDFs for the latest 6 months
        from the GNA 2022 updates page.

        Target : https://www.ctuil.in/gna2022updates
        Output : uploads/CTUIL-GNA-Connectivity-Fresh/
        """
        from app.scrapers import source_12_ctuil_gna_connectivity_fresh_scraper as script

        return execute_scraper(
            script,
            label="GNA Connectivity Fresh",
            output_dir="uploads/CTUIL-GNA-Connectivity-Fresh",
        )
