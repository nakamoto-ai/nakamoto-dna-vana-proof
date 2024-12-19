import gc
import json
import logging
import requests
import random
from collections import defaultdict
from typing import List, Tuple, Dict, Any

import pandas as pd
import numpy as np

from dna_vana_proof.exception import raise_custom_exception


class DbSNPHandler:

    def __init__(self, config: Dict[str, Any]):
        self.config = config

    @staticmethod
    def is_i_rsid(rsid: str) -> bool:
        return rsid.startswith("i") and rsid[1:].isdigit()

    @staticmethod
    def is_indel(genotype: str) -> bool:
        return genotype == "--" or any(special in genotype.upper() for special in ["I", "D"])

    def handle_special_cases(
        self,
        rsid_array: np.ndarray,
        genotype_array: np.ndarray,
        invalid_genotypes: List[str],
        indels: List[str],
        i_rsids: List[str],
    ) -> Tuple[List[str], List[str], List[str]]:
        i_rsid_mask = np.vectorize(lambda x: self.is_i_rsid(x))(rsid_array)
        indel_mask = np.isin(genotype_array, ["--", "II", "DD"])

        indels.extend(rsid_array[indel_mask & np.isin(rsid_array, invalid_genotypes)].tolist())
        invalid_genotypes = list(
            set(invalid_genotypes) - set(rsid_array[indel_mask & np.isin(rsid_array, invalid_genotypes)])
        )

        i_rsids.extend(rsid_array[i_rsid_mask & np.isin(rsid_array, invalid_genotypes)].tolist())
        invalid_genotypes = list(
            set(invalid_genotypes) - set(rsid_array[i_rsid_mask & np.isin(rsid_array, invalid_genotypes)])
        )

        return indels, i_rsids, invalid_genotypes

    def verify_snp(self, rsid: str | None, genotype: str) -> Tuple[None | str, None | str, None | str]:
        if rsid is None:
            return rsid, None, None

        if self.is_i_rsid(rsid):
            return None, None, rsid

        if self.is_indel(genotype):
            return None, genotype, None

        return rsid, None, None

    def check_indels_and_i_rsids(
        self,
        rsid_list: List[str],
        genotype_list: List[str],
        invalid_genotypes: List[str],
        indels: List[str],
        i_rsids: List[str],
    ) -> Dict[str, int | List[Any]]:
        rsid_array = np.array(rsid_list)
        genotype_array = np.array(genotype_list)

        indels, i_rsids, invalid_genotypes = self.handle_special_cases(
            rsid_array, genotype_array, invalid_genotypes, indels, i_rsids
        )

        dna_info = {
            "indels": len(indels),
            "i_rsids": len(i_rsids),
            "invalid_genotypes": len(invalid_genotypes),
            "all": len(indels) + len(i_rsids) + len(invalid_genotypes),
        }

        return dna_info

    def verify_snps(self, df: pd.DataFrame) -> Tuple[List[str | None]]:
        sampled_rsids = self.get_sampled_rsids(df)
        genome_response, status_code = self.verify_genome(sampled_rsids)

        if isinstance(genome_response, str):
            message = (
                "We are experiencing issues with our genome quality check API."
                "Please try again and contact administrators if issue persists."
            )
            raise_custom_exception(
                error_type="Genome Quality API Error",
                message=message,
                status_code=status_code,
                response=genome_response,
            )

        invalid_list = genome_response.get("invalid", [])
        rsid_list = [item["rsid"] for item in invalid_list]
        genotype_list = ["".join(item["genotype"]) for item in invalid_list]

        results = genome_response.get("valid", [])

        skipped_rsids = []
        indels = []
        i_rsids = []

        for rsid, genotype in zip(rsid_list, genotype_list):
            skipped_rsid, indel, i_rsid = self.verify_snp(rsid, genotype)
            if skipped_rsid:
                skipped_rsids.append(skipped_rsid)
            if indel:
                indels.append(indel)
            if i_rsid:
                i_rsids.append(i_rsid)

        return results, skipped_rsids, indels, i_rsids

    def get_sampled_rsids(self, df: pd.DataFrame) -> List[Dict[str, str | List[str]]]:
        rsid_list = df["rsid"].tolist()
        genotype_list = df["genotype"].tolist()
        chromosomes = df["chromosome"].tolist()

        grouped_data = defaultdict(list)
        for rsid, genotype, chrom in zip(rsid_list, genotype_list, chromosomes):
            grouped_data[chrom].append((rsid, genotype))

        sampled_rsids = []
        chromosome_names = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]

        for chrom in chromosome_names:
            if chrom in grouped_data:
                selected_items = random.sample(grouped_data[chrom], min(10, len(grouped_data[chrom])))

                for rsid, genotype in selected_items:
                    allele_list = list(set(genotype))

                    item_dict = {"rsid": rsid, "genotype": allele_list}
                    sampled_rsids.append(item_dict)

        return sampled_rsids

    def verify_genome(
        self, final_list: List[Dict[str, str | List[str]]]
    ) -> Dict[str, List[Dict[str, str | List[str]]]]:
        token = self.config["token"]
        endpoint = self.config["endpoint"]

        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
        }

        data = json.dumps({"genomes": final_list})

        response = requests.get(url=endpoint, data=data, headers=headers)
        genome_response = response
        status_code = response.status_code
        if 200 <= response.status_code < 300:
            return genome_response.json(), status_code
        else:
            return genome_response.text, status_code

    def load_data(self, filepath: str) -> pd.DataFrame:
        return pd.read_csv(
            filepath,
            comment="#",
            sep="\s+",
            names=["rsid", "chromosome", "position", "genotype"],
            dtype={"rsid": str, "chromosome": str, "position": int, "genotype": str},
        )

    def filter_valid_chromosomes(self, df: pd.DataFrame) -> Tuple[pd.DataFrame, List[str], List[str]]:
        valid_chromosomes = [str(i) for i in range(1, 23)] + ["X", "Y", "MT"]

        df["chromosome"] = df["chromosome"].astype(str).str.strip()

        df_valid = df[df["chromosome"].isin(valid_chromosomes)]
        invalid_chromosomes = df[~df["chromosome"].isin(valid_chromosomes)]

        unique_invalid_chromosomes = []
        if not invalid_chromosomes.empty:
            unique_invalid_chromosomes += invalid_chromosomes["chromosome"].unique().tolist()
            logging.Info(f"Invalid chromosomes found: {', '.join(unique_invalid_chromosomes)}")

        unique_chromosomes_in_df = df["chromosome"].unique()
        missing_chromosomes = list(set(valid_chromosomes) - set(unique_chromosomes_in_df))

        return df_valid, unique_invalid_chromosomes, missing_chromosomes

    def check_genotypes(self, df_valid: pd.DataFrame) -> Dict[str, int | List[Any]]:
        dbsnp_verified, invalid_genotypes, indels, i_rsids = self.verify_snps(df_valid)

        rsid_list = df_valid["rsid"].tolist()
        genotype_list = df_valid["genotype"].tolist()

        dna_info = self.check_indels_and_i_rsids(rsid_list, genotype_list, invalid_genotypes, indels, i_rsids)

        dna_info["dbsnp_verified"] = len(dbsnp_verified)
        dna_info["all"] += len(dbsnp_verified)

        return dna_info

    def dbsnp_verify(self, filepath: str) -> Dict[str, Any]:
        df = self.load_data(filepath)
        df_valid, invalid_chromosomes, missing_chromosomes = self.filter_valid_chromosomes(df)
        dna_info = self.check_genotypes(df_valid)
        dna_info["invalid_chromosomes"] = invalid_chromosomes
        dna_info["missing_chromosomes"] = missing_chromosomes

        del df, df_valid
        gc.collect()

        return dna_info
