import json
import gc
from pathlib import Path
from langchain_community.document_loaders import DirectoryLoader
from langchain_unstructured import UnstructuredLoader
from unstructured.cleaners.core import clean_extra_whitespace

# --- Config ---
BATCH_SIZE = 50   # chunks per JSONL file
OUTPUT_DIR = Path("data/ingested/chunk_batches")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- Directory Loader ---
loader = DirectoryLoader(
    "data/pdf",
    glob="**/*.pdf",
    loader_cls=UnstructuredLoader,
    loader_kwargs={
        # 🔑 NEW WAY
        "chunking_strategy": "basic",
        "max_characters": 1000,          # target chunk size
        "include_orig_elements": False,

        # extraction options
        "strategy": "fast",
        "languages": ["en"],
        "post_processors": [clean_extra_whitespace],
    },
)

batch = []
batch_id = 0

for doc in loader.lazy_load():
    record = {
        "text": doc.page_content,
        "metadata": doc.metadata,
    }
    batch.append(record)

    # 🔍 LOG OPENED FILE
    source = doc.metadata.get("source", "unknown")
    print(f"Processed chunk from: {source}")

    if len(batch) >= BATCH_SIZE:
        out_file = OUTPUT_DIR / f"batch_{batch_id:05d}.jsonl"
        with out_file.open("w", encoding="utf-8") as f:
            for r in batch:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")

        print(f"✅ Saved {out_file} ({len(batch)} chunks)")
        batch.clear()
        gc.collect()
        batch_id += 1

# Save remaining
if batch:
    out_file = OUTPUT_DIR / f"batch_{batch_id:05d}.jsonl"
    with out_file.open("w", encoding="utf-8") as f:
        for r in batch:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
    print(f"✅ Saved {out_file} ({len(batch)} chunks)")

print("🎉 Ingestion complete")