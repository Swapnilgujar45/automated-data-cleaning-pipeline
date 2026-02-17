import pandas as pd
import logging

logger = logging.getLogger("VALIDATION")

def validation_schema(df: pd.DataFrame, expected_cols:list):
    logging.info("Running schema validation")

    missing_cols = set(expected_cols) - set(df.columns)

    if missing_cols:
        logging.error(f"Missing Columns: {missing_cols}")
        raise ValueError(f"misisng columns: {missing_cols}")

    logging.info("Schema validation passed")

def validation_business_rule(df: pd.DataFrame):

    logging.info("Running business validation")

    invalid_age = df[(df['age']<=0) | (df['age']>100)]
    invalid_salary = df[df['salary'] <=0]
    invalid_email = df[~df['email'].astype(str).str.contains("@", na=False)]

    logging.info(f"Invalid age records: {len(invalid_age)}")
    logging.info(f"Invalid salary records: {len(invalid_salary)}")
    logging.info(f"Invalid_email records: {len(invalid_email)}")

    df['validation_flag'] = 'PASS'

    df.loc[invalid_age.index, 'validation_flag'] = 'FAIL'
    df.loc[invalid_salary.index, 'validation_flag'] = 'FAIL'
    df.loc[invalid_email.index, 'validation_flag'] = 'FAIL'

    return df
    