import logging
import os
import requests
import gc
import re
import hashlib
from typing import Dict, Any, List

import pandas as pd

from dna_vana_proof.models.proof_response import ProofResponse
from dna_vana_proof.verify import DbSNPHandler


class TwentyThreeWeFileScorer:
    header_template = """
    # This file contains raw genotype data, including data that is not used in 23andMe reports.
    # This data has undergone a general quality review however only a subset of markers have been 
    # individually validated for accuracy. As such, this data is suitable only for research, 
    # educational, and informational use and not for medical or other use.
    # 
    # Below is a text version of your data.  Fields are TAB-separated
    # Each line corresponds to a single SNP.  For each SNP, we provide its identifier 
    # (an rsid or an internal id), its location on the reference human genome, and the 
    # genotype call oriented with respect to the plus strand on the human reference sequence.
    # We are using reference human assembly build 37 (also known as Annotation Release 104).
    # Note that it is possible that data downloaded at different times may be different due to ongoing 
    # improvements in our ability to call genotypes. More information about these changes can be found at:
    #
    # More information on reference human assembly builds:
    # https://www.ncbi.nlm.nih.gov/assembly/GCF_000001405.13/
    #
    # rsid	chromosome	position	genotype
    """
    valid_genotypes = set("ATCG-ID")
    valid_chromosomes = set([str(i) for i in range(1, 23)] + ["X", "Y", "MT"])

    def __init__(self, input_data: List[str], config: Dict[str, Any]):
        self.input_data = input_data
        self.profile_id = self.get_profile_id(input_data)
        self.config = config
        self.proof_response = None
        self.hash = None
        self.sender_address = None

    @staticmethod
    def get_profile_id(input_data: List[str]) -> str | None:
        file_content = "\n".join([d for d in input_data[:50]])

        url_prefix = "https://you.23andme.com/p/"
        url_suffix = "/tools/data/download/"

        start_index = file_content.find(url_prefix)
        if start_index == -1:
            return None

        end_index = file_content.find(url_suffix, start_index)
        if end_index == -1:
            return None

        profile_id_start = start_index + len(url_prefix)
        profile_id = file_content[profile_id_start:end_index]

        if profile_id:
            return profile_id
        else:
            return None

    def read_header(self) -> str:
        header_lines = []
        for line in self.input_data:
            if line.startswith("#") or line.startswith("rsid"):
                if re.match(r"# This data file generated by 23andMe at:", line):
                    continue
                if re.match(r"# https://you\.23andme\.com/p/", line):
                    continue
                header_lines.append(line.strip())
            else:
                break
        header = "\n".join(header_lines[1:])
        return header

    def check_header(self) -> bool:
        file_header = self.read_header()

        clean_template = "\n".join(
            [
                line.strip()
                for line in self.header_template.strip().split("\n")
                if """https://you.23andme.com""" not in line
            ]
        )
        clean_file_header = file_header.strip()

        return clean_template == clean_file_header

    def check_rsid_lines(self) -> bool:
        invalid_rows = []

        line_number = 1
        for line in self.input_data:
            line = line.strip()

            if "#" in line:
                continue

            if not line:
                logging.info(f"File ended unexpectedly at line {line_number}.")
                return False

            columns = line.split("\t")
            if len(columns) != 4:
                logging.info(f"Line {line_number} does not have exactly 4 columns: {line}")
                return False

            rsid, chromosome, position, genotype = columns
            row = (rsid, chromosome, position, genotype)

            if not re.match(r"^(rs|i)\d+$", rsid):
                logging.info(f"Line {line_number}: Invalid rsid format: {rsid}")
                invalid_rows.append(row)

            if chromosome not in self.valid_chromosomes:
                logging.info(f"Line {line_number}: Invalid chromosome value: {chromosome}")
                invalid_rows.append(row)

            if any(char not in self.valid_genotypes for char in genotype):
                logging.info(f"Line {line_number}: Invalid genotype characters: {genotype}")
                invalid_rows.append(row)

            line_number += 1

        if invalid_rows:
            return False

        return True

    def verify_profile(self) -> bool:
        """
        Sends the profile id for verification via a POST request.
        """
        self.sender_address = self.config["verify"].split("address=")[-1]
        url = f"{self.config['verify']}&profile_id={self.profile_id}"
        response = requests.get(url=url)
        resp = response.json()
        profile_verified = resp.get("is_approved", False)

        return profile_verified

    def verify_hash(self, genome_hash: str) -> bool:
        """
        Sends the hashed genome data for verification via a POST request.
        """
        url = f"{self.config['key']}&genome_hash={genome_hash}"
        response = requests.get(url=url)
        resp = response.json()
        hash_unique = resp.get("is_unique", False)

        return hash_unique

    def hash_23andme_file(self, file_path: str) -> str:
        df = pd.read_csv(
            file_path,
            sep="\t",
            comment="#",
            names=["rsid", "chromosome", "position", "genotype"],
        )

        df_filtered = df[~df["rsid"].str.startswith("i")]

        df_sorted = df_filtered.sort_values(by="rsid")

        concatenated_string = "|".join(
            df_sorted.apply(
                lambda row: f"{row['rsid']}:{row['chromosome']}:{row['position']}:{row['genotype']}",
                axis=1,
            )
        )

        del df, df_filtered, df_sorted
        gc.collect()

        hash_object = hashlib.sha256(concatenated_string.encode())
        hash_hex = hash_object.hexdigest()
        self.hash = hash_hex

        return hash_hex

    @staticmethod
    def invalid_genotypes_score(total: int, low: int = 1, high: int = 5) -> float:
        if total <= low:
            return 1.0
        elif total >= high:
            return 0.0
        else:
            return 1.0 - (total - low) / (high - low)

    @staticmethod
    def indel_score(
        total: int,
        low: int = 3,
        ultra_low: int = 1,
        high: int = 7,
        ultra_high: int = 25,
    ) -> float:
        if total <= ultra_low:
            return 0.0
        elif ultra_low < total <= low:
            return (total - ultra_low) / (low - ultra_low)
        elif low < total <= high:
            return 1.0
        elif high < total <= ultra_high:
            return (ultra_high - total) / (ultra_high - high)
        else:
            return 0.0

    @staticmethod
    def i_rsid_score(total: int, low: int = 5, high: int = 30) -> float:
        if total <= low:
            return 1.0
        elif total >= high:
            return 0.0
        else:
            return 0.5

    @staticmethod
    def percent_verification_score(
        verified: int,
        all: int,
        low: float = 0.9,
        ultra_low: float = 0.85,
        high: float = 0.96,
        ultra_high: float = 0.98,
    ) -> float:
        verified_ratio = verified / all

        if low <= verified_ratio <= high:
            return 1.0
        elif ultra_low < verified_ratio < low:
            return (verified_ratio - ultra_low) / (low - ultra_low)
        elif high < verified_ratio <= ultra_high:
            return (ultra_high - verified_ratio) / (ultra_high - high)
        elif verified_ratio > ultra_high:
            return 0.0
        else:
            return 0.0

    def save_hash(self, proof_response: ProofResponse) -> bool:
        hash_data = self.generate_hash_save_data(proof_response)
        response = requests.post(url=self.config["key"], data=hash_data)
        resp = response.json()
        success = resp.get("success", False)

        return success

    def generate_hash_save_data(self, proof_response: ProofResponse) -> Dict[str, Any]:
        hash_save_data = {
            "sender_address": self.sender_address,
            "attestor_address": "",
            "tee_url": "",
            "job_id": "",
            "file_id": "",
            "profile_id": self.profile_id,
            "genome_hash": self.hash,
            "authenticity_score": proof_response.authenticity,
            "ownership_score": proof_response.ownership,
            "uniqueness_score": proof_response.uniqueness,
            "quality_score": proof_response.quality,
            "total_score": proof_response.attributes["total_score"],
            "score_threshold": proof_response.attributes["score_threshold"],
            "is_valid": proof_response.valid,
        }

        return hash_save_data

    def proof_of_ownership(self) -> float:
        validated = self.verify_profile()

        if validated:
            return 1.0
        else:
            return 0

    def proof_of_quality(self, filepath) -> float:
        dbsnp = DbSNPHandler(self.config)
        results = dbsnp.dbsnp_verify(filepath)

        invalid_score = self.invalid_genotypes_score(results["invalid_genotypes"])
        indel_score = self.indel_score(results["indels"])
        i_rsid_score = self.i_rsid_score(results["i_rsids"])
        percent_verify_score = self.percent_verification_score(results["dbsnp_verified"], results["all"])

        quality_score = 0.4 * invalid_score + 0.3 * percent_verify_score + 0.2 * indel_score + 0.1 * i_rsid_score

        return quality_score

    def proof_of_uniqueness(self, filepath) -> float:
        hashed_dna = self.hash_23andme_file(filepath)
        unique = self.verify_hash(hashed_dna)

        if unique:
            return 1.0
        else:
            return 0

    def proof_of_authenticity(self) -> float:
        header_ok = self.check_header()
        rsids_ok = self.check_rsid_lines()

        if header_ok and rsids_ok:
            return 1.0
        else:
            return 0


class Proof:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.proof_response = ProofResponse(dlp_id=config["dlp_id"])
        self.proof_response.authenticity = 0
        self.proof_response.ownership = 0
        self.proof_response.uniqueness = 0
        self.proof_response.quality = 0

    def update_proof_response(self, scorer: TwentyThreeWeFileScorer, twenty_three_file: str):
        self.proof_response.authenticity = scorer.proof_of_authenticity()
        if self.proof_response.authenticity <= 0:
            return

        self.proof_response.ownership = scorer.proof_of_ownership()
        if self.proof_response.ownership <= 0:
            self.reset_scores()
            return

        self.proof_response.uniqueness = scorer.proof_of_uniqueness(filepath=twenty_three_file)
        if self.proof_response.uniqueness <= 0:
            self.reset_scores()
            return

        self.proof_response.quality = scorer.proof_of_quality(filepath=twenty_three_file)

    def reset_scores(self):
        self.proof_response.authenticity = 0
        self.proof_response.uniqueness = 0
        self.proof_response.ownership = 0
        self.proof_response.quality = 0

    def generate(self) -> ProofResponse:
        logging.info("Starting proof generation")

        input_filename = os.listdir(self.config["input_dir"])[0]

        input_file = os.path.join(self.config["input_dir"], input_filename)
        with open(input_file, "r") as i_file:
            twenty_three_file = input_file
            input_data = [f for f in i_file]
            scorer = TwentyThreeWeFileScorer(input_data=input_data, config=self.config)

        score_threshold = 0.9

        self.update_proof_response(scorer, twenty_three_file)

        total_score = (
            0.25 * self.proof_response.quality
            + 0.25 * self.proof_response.ownership
            + 0.25 * self.proof_response.authenticity
            + 0.25 * self.proof_response.uniqueness
        )

        if total_score < score_threshold:
            self.reset_scores()
            total_score = 0

        self.proof_response.score = total_score
        self.proof_response.valid = total_score >= score_threshold

        self.proof_response.attributes = {
            "total_score": total_score,
            "score_threshold": score_threshold,
        }

        self.proof_response.metadata = {
            "dlp_id": self.config["dlp_id"],
        }

        if self.proof_response.valid:
            save_successful = scorer.save_hash(self.proof_response)

            if save_successful:
                logging.info("Hash Data Saved Successfully.")
            else:
                raise Exception("Hash Data Saving Failed.")

        return self.proof_response