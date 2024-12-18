import json
import logging
import os
import sys
import traceback
from typing import Dict, Any

from dna_vana_proof.proof import Proof


INPUT_DIR, OUTPUT_DIR = "/input", "/output"

logging.basicConfig(level=logging.INFO, format="%(message)s")


def load_config() -> Dict[str, Any]:
    config = {
        "dlp_id": 2,
        "input_dir": INPUT_DIR,
        "token": os.environ.get("TOKEN", None),
        "key": os.environ.get("KEY", None),
        "verify": os.environ.get("VERIFY", None),
        "endpoint": os.environ.get("ENDPOINT", None),
    }

    return config


def run() -> None:
    config = load_config()
    input_files_exist = os.path.isdir(INPUT_DIR) and bool(os.listdir(INPUT_DIR))

    if not input_files_exist:
        raise FileNotFoundError(f"No input files found in {INPUT_DIR}")

    change_filename_if_zip()

    proof = Proof(config)
    proof_response = proof.generate()

    output_path = os.path.join(OUTPUT_DIR, "results.json")

    with open(output_path, "w") as f:
        json.dump(proof_response.dict(), f, indent=2)

    logging.info(f"Proof generation complete: {proof_response}")


def change_filename_if_zip():
    input_file = [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))][0]
    filepath = os.path.join(INPUT_DIR, input_file)

    if filepath.lower().endswith(".zip"):
        new_file_path = os.path.splitext(filepath)[0] + ".txt"
        os.rename(filepath, new_file_path)
        return f"Input file renamed to: {new_file_path}"
    else:
        return "Input file is not a ZIP file."


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.error(f"Error during proof generation: {e}")
        traceback.print_exc()
        sys.exit(1)
