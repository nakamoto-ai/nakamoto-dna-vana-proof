import json
import logging
import os
import sys
import traceback
import zipfile
import shutil
import uuid
from datetime import datetime
from typing import Dict, Any

from dna_vana_proof.proof import Proof


INPUT_DIR, OUTPUT_DIR = "/input", "/output"

logging.basicConfig(level=logging.INFO, format="%(message)s")

log_data = {
    "id": None,
    "success": False,
    "info": {
        "start_time": None,
        "end_time": None,
        "address": None,
        "profile_id": None,
        "reason": None,
        "error": False,
        "message": None,
        "scores": {"total": 0, "authenticity": 0, "ownership": 0, "quality": 0, "uniqueness": 0},
    },
}


def generate_job_id() -> str:
    timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    unique_id = uuid.uuid4().hex[:8]
    return f"{timestamp}-{unique_id}"


def generate_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def send_logs(log_data):
    # request to logging api here
    return


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
    log_data["id"] = generate_job_id()
    log_data["info"]["start_time"] = generate_timestamp()
    config = load_config()
    input_files_exist = os.path.isdir(INPUT_DIR) and bool(os.listdir(INPUT_DIR))

    if not input_files_exist:
        msg = f"No input files found in {INPUT_DIR}"

        log_data["info"]["end_time"] = generate_timestamp()
        log_data["info"]["reason"] = "File Not Found Error"
        log_data["info"]["error"] = True
        log_data["info"]["message"] = msg
        send_logs(log_data)
        raise FileNotFoundError(msg)

    change_filename_if_zip()

    proof = Proof(config, log_data)
    proof_response = proof.generate()

    output_path = os.path.join(OUTPUT_DIR, "results.json")

    with open(output_path, "w") as f:
        json.dump(proof_response.dict(), f, indent=2)

    logging.info(f"Proof generation complete: {proof_response}")


def change_filename_if_zip():
    input_file = [f for f in os.listdir(INPUT_DIR) if os.path.isfile(os.path.join(INPUT_DIR, f))][0]
    filepath = os.path.join(INPUT_DIR, input_file)

    if filepath.lower().endswith(".zip"):
        try:
            with zipfile.ZipFile(filepath, "r") as zip_ref:
                zip_ref.testzip()

                temp_dir = os.path.join(INPUT_DIR, "temp_unzip")
                os.makedirs(temp_dir, exist_ok=True)
                zip_ref.extractall(temp_dir)

            for root, _, files in os.walk(temp_dir):
                if files:
                    first_file = files[0]
                    source_path = os.path.join(root, first_file)
                    destination_path = os.path.join(INPUT_DIR, first_file)
                    shutil.move(source_path, destination_path)

                    os.remove(filepath)
                    shutil.rmtree(temp_dir)

                    print(f"ZIP file unzipped and first file moved to: {destination_path}")

            shutil.rmtree(temp_dir)
            print("ZIP file was empty.")

        except zipfile.BadZipFile:
            new_file_path = os.path.splitext(filepath)[0] + ".txt"
            os.rename(filepath, new_file_path)
            print(f"Invalid ZIP file renamed to: {new_file_path}")
    else:
        print("Input file is not a ZIP file.")


if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logging.error(f"Error during proof generation: {e}")
        traceback.print_exc()
        sys.exit(1)
