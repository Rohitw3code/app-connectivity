import pdfplumber, glob, os, json

bay_dir = "source/bayallocation"
pdfs = list(glob.glob(os.path.join(bay_dir, "*.pdf")))
pdf_path = pdfs[0]

with pdfplumber.open(pdf_path) as pdf:
    page = pdf.pages[0]
    tables = page.extract_tables()
    tbl = tables[0]
    print(f"Table: {len(tbl)} rows x {len(tbl[0])} cols")
    # rows 5..10
    for i, row in enumerate(tbl[5:12], start=5):
        print(f"row[{i}]: {json.dumps(row, ensure_ascii=False)}")
