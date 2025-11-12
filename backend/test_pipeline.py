import sys
import mimetypes
from pathlib import Path

import requests
from app.core.config import settings

def gather_dataset_files(dataset_dir: Path):
    exts = {".csv", ".xlsx", ".xls"}
    files = [p for p in sorted(dataset_dir.iterdir()) if p.suffix.lower() in exts and p.is_file()]
    return files

def main():
    # Resolve dataset folder relative to this script (backend/dataset)
    base = Path(__file__).resolve().parent
    dataset_dir = base / "dataset"
    if not dataset_dir.exists():
        print("dataset folder not found:", dataset_dir)
        sys.exit(2)

    files = gather_dataset_files(dataset_dir)
    if not files:
        print("no dataset files found in", dataset_dir)
        sys.exit(3)

    host = settings.api_host
    if host == "0.0.0.0":
        host = "127.0.0.1"
    url = f"http://{host}:{settings.api_port}/upload"

    # Prepare multipart files payload (API accepts one or more files)
    opened = []
    multipart = []
    try:
        for p in files:
            f = open(p, "rb")
            opened.append(f)
            ctype = mimetypes.guess_type(p.name)[0] or "application/octet-stream"
            multipart.append(("files", (p.name, f, ctype)))

        print("uploading", len(multipart), "file(s) to", url)
        resp = requests.post(url, files=multipart, timeout=60)
        print("status:", resp.status_code)
        try:
            print("response:", resp.json())
        except Exception:
            print("response text:", resp.text)
    except Exception as e:
        print("error during upload:", e)
        sys.exit(4)
    finally:
        for f in opened:
            f.close()

if __name__ == "__main__":
    main()
