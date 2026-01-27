import os
import json
import gc
from langchain_unstructured import UnstructuredLoader
from unstructured.cleaners.core import clean_extra_whitespace

POPPLER_PATH = r"C:\path\to\poppler\bin"

PDF_FOLDER = r"data\pdf"
BATCH_SAVE_FOLDER = r"data\ingested\batches"
os.makedirs(BATCH_SAVE_FOLDER, exist_ok=True)

def get_next_batch_number(save_folder):
    """Get the next batch number based on existing files in the folder."""
    existing = [f for f in os.listdir(save_folder) if f.startswith("batch_") and f.endswith(".jsonl")]
    if not existing:
        return 1
    # extract numbers from filenames
    numbers = [int(f.replace("batch_", "").replace(".jsonl", "")) for f in existing]
    return max(numbers) + 1

def process_pdf(pdf_filename, save_folder, start_batch_num):
    pdf_path = os.path.join(PDF_FOLDER, pdf_filename)
    
    try:
        loader = UnstructuredLoader(
            pdf_path,
            mode="elements",
            strategy="fast",
            post_processors=[clean_extra_whitespace],
            languages=["eng"],
            poppler_path=POPPLER_PATH,
        )
        
        docs = loader.load()
        
        # Save batches as sequentially numbered files
        batch_file = os.path.join(save_folder, f"batch_{start_batch_num}.jsonl")
        with open(batch_file, "w", encoding="utf-8") as f:
            for doc in docs:
                json.dump({"text": doc.page_content, "metadata": doc.metadata}, f)
                f.write("\n")
        
        print(f"Processed and saved: {pdf_filename} → {len(docs)} elements as batch_{start_batch_num}.jsonl")
    
    except Exception as e:
        print(f"Error processing {pdf_filename}: {e}")
    
    finally:
        try:
            del docs
        except NameError:
            pass
        del loader
        gc.collect()

# List all PDF files
all_pdfs = sorted([f for f in os.listdir(PDF_FOLDER) if f.lower().endswith(".pdf")])

# Skip first 536 PDFs
remaining_pdfs = all_pdfs[536:]
print(f"Total PDFs remaining: {len(remaining_pdfs)}")

# Get starting batch number
batch_num = get_next_batch_number(BATCH_SAVE_FOLDER)

# Process remaining PDFs
for i, pdf_file in enumerate(remaining_pdfs, start=1):
    print(f"\nProcessing PDF {i}/{len(remaining_pdfs)}: {pdf_file}")
    process_pdf(pdf_file, BATCH_SAVE_FOLDER, batch_num)
    batch_num += 1  # increment batch number for next PDF

print("\nAll remaining PDFs processed.")