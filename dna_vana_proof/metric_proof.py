import logging
import os
import requests
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List

from dna_vana_proof.models.proof_response import ProofResponse


def validate_weight(weight: Any) -> bool:
    return _validate_integer_gt(weight, 0)


def validate_steps(steps: Any) -> bool:
    return _validate_integer_gt(steps, -1)


def _validate_integer_gt(v: Any, t: int) -> bool:
    """
    Validates that the given value `v` is of type `int` and is greater than threshold `t`.
    """
    return isinstance(v, int) and v > t


# def _tx_filter(tx):
#     """
#     Determines if the transaction is a `requestReward` method call to the smart contract
#     `0xe1Aa905aBF3CC018832c038c636FF7041923C8d4`, within the last 24 hours. If it is, True
#     is returned, otherwise False.
#
#     If True is returned, the user potentially was rewarded by the DNA DLP within the last
#     24 hours.
#     """
#     tx_time = datetime.strptime(tx["timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ")
#     tx_time = tx_time.replace(tzinfo=timezone.utc)
#     tx_method = tx["method"]
#     tx_to = tx["to"]["hash"]
#
#     now = datetime.now(timezone.utc)
#     target_to = "0xe1Aa905aBF3CC018832c038c636FF7041923C8d4"
#     target_method = "requestReward"
#
#     if now - timedelta(hours=24) > tx_time:
#         return False
#
#     if tx_method != target_method:
#         return False
#
#     if tx_to != target_to:
#         return False
#
#     return True
#


class MetricProof:

    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proof_response = ProofResponse(dlp_id=config["dlp_id"])

    def generate(self) -> ProofResponse:
        """
        Generates a proof response for the DNA Metric Proof on Vana. 

        Valid submissions will contain either the users weight, number of steps for the day,
        or both. Submitting both metrics will give a score of `100%`, only one will give `50%`,
        and none will give `0%`. Metrics can only be submitted once every 24 hours. 

        The result is returned as a `ProofResponse`.
        """
        logging.info("Starting proof generation")

        now = datetime.now(timezone.utc)
        past = now - timedelta(hours=24)
        t = past.strftime("%Y-%m-%dT%H:%M:%SZ")

        resp = requests.get(
            f'{self.config["api_url"]}&filter=proof_type=metrics&filter=create_date>{t}&filter=sender_address={self.config["address"]}'
        )
        resp.raise_for_status()

        data = resp.json()
        logging.info(f"Found {len(data)} proofs.")
        if len(data) > 0:
            logging.info("Address is throttled... Score: 0%")
            self.proof_response.valid = False
            return self.proof_response

        input_filename = os.listdir(self.config["input_dir"])[0]
        input_file = os.path.join(self.config["input_dir"], input_filename)
        with open(input_file, "r") as file:
            data = json.load(file)

        valid_weight = "weight" in data and validate_weight(data["weight"])
        valid_steps = "steps" in data and validate_steps(data["steps"])

        if valid_weight and valid_steps:
            logging.info("Score: 100%")
            self.proof_response.score = 1.0
        elif valid_weight or valid_steps:
            logging.info("Score: 50%")
            self.proof_response.score = 0.5
        else:
            logging.info("Score: 0%... :(")
            self.proof_response.valid = False
            return self.proof_response

        self.proof_response.score = self.proof_response.score / 100
        self.proof_response.valid = True
        self.proof_response.authenticity = 1.0
        self.proof_response.ownership = 1.0
        self.proof_response.quality = 1.0
        self.proof_response.uniqueness = 1.0

        requests.post(
            self.config["api_url"],
            data={
                "sender_address": self.config["address"],
                "file_id": self.config["file_id"],
                "proof_type": "metrics"
            }
        )

        return self.proof_response
