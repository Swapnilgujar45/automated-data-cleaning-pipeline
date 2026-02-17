import argparse
import logging 
from pathlib import Path

from utils.logger import setup_logger
from utils.config_loader import load_config
from ingestion.bronze import load_bronze
from processing.silver import silver_transformation
from processing.gold import gold_transformation
from validations.rules import validation_schema, validation_business_rule

def main():
    setup_logger()
    logger = logging.getLogger("MAIN")

    parser = argparse.ArgumentParser(description = "Medallion ETL Pipeline")
    parser.add_argument("--input", required = True)
    args = parser.parse_args()

    config = load_config()

    logging.info("Pipeline Started")

    df_bronze = load_bronze(args.input)

    validation_schema(df_bronze, config["schema"]["expected_columns"])

    df_silver = silver_transformation(df_bronze)
    df_silver = validation_business_rule(df_silver)

    Path("data/silver").mkdir(parents = True, exist_ok = True)
    df_silver.to_parquet("data/silver/silver_clean.parquet", index= False)

    df_gold = gold_transformation(df_silver)

    Path("data/gold").mkdir(parents = True, exist_ok = True)
    df_gold.to_parquet("data/gold/gold_analystics.parquet", index = True)

    logging.info("Pipeline Completed Successfully")

if __name__ == '__main__':
    main()