"""
pipeline/downloader/ — PDF Download Module
============================================
Merged from ctuil-pdf-scraper-main.
Downloads PDFs for each handler type and registers them in the tracker.

Downloaders with pipeline extraction handlers:
  - cmets_downloader      → source/cmets_pdfs/
  - jcc_downloader        → source/jcc_pdfs/
  - effectiveness_downloader → source/effectiveness_pdfs/
  - bayallocation_downloader → source/bayallocation/

Downloaders without extraction (kept as-is):
  - reallocation, bidding, compliance, monitoring, transmission,
    bulk_consumers, gna_fresh, cea_500gw, cea_nct, pfcclindia
"""

from pipeline.downloader.cmets_downloader import download_cmets_pdfs
from pipeline.downloader.jcc_downloader import download_jcc_pdfs
from pipeline.downloader.effectiveness_downloader import download_effectiveness_pdfs
from pipeline.downloader.bayallocation_downloader import download_bayallocation_pdfs

__all__ = [
    "download_cmets_pdfs",
    "download_jcc_pdfs",
    "download_effectiveness_pdfs",
    "download_bayallocation_pdfs",
]
