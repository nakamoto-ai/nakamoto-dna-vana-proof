import json
import logging
import os
import sys
import traceback
from typing import Dict, Any

from dna_vana_proof.proof import Proof
from dna_vana_proof.exception import raise_custom_exception


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

    missing_env_vars = [
        env_var.upper() for env_var in ["token", "key", "verify", "endpoint"] if not config.get(env_var)
    ]
    if missing_env_vars:
        message = (
            f"Missing environment variable(s): {', '.join(missing_vars.upper())}. "
            f"Please contact administrators for further assistance."
        )
        raise_custom_exception(error_type="Missing Environment Variables", message=message)

    input_files_exist = os.path.isdir(INPUT_DIR) and bool(os.listdir(INPUT_DIR))
    if not input_files_exist:
        message = (
            f"Genome file is missing or improperly uploaded."
            f"Please try again or contact administrators for assistance."
        )
        raise_custom_exception(error_type="Missing Genome File", message=message)

    change_filename_if_zip()

    valid_filetypes = ["txt"]
    filetype = get_filetype()
    if filetype not in valid_filetypes:
        message = f"Genome file cannot be type '.{filetype}'. Must be one of following types: {valid_filetypes}"
        raise_custom_exception(error_type="Invalid File Type", message=message)

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
        logging.debug(f"Input file renamed to: {new_file_path}")
    else:
        logging.debug("Input file is not a ZIP file.")


def get_filetype():
    input_file = [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))][0]
    extension = input_file.split(".")[-1]
    return extension


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.error(f"Error during proof generation: {e}")
        traceback.print_exc()
        sys.exit(1)
