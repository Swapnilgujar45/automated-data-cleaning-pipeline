import pandas as pd
import logging
import re

logger = logging.getLogger(__name__)

def clean_text_fields(df):
    
    logging.info("Cleaning Text Columns")

    df['name'] = df['name'].astype(str).str.replace(r'[^a-zA-Z ]', '', regex= True).str.strip()
    df['city'] = df['city'].astype(str).str.strip().str.title()
    df['country'] = df['country'].astype(str).str.strip().str.title()

    return df

def clean_numeric_fields(df):

    logging.info("Cleaning Numeric Columns")

    df['age'] = pd.to_numeric(df['age'], errors = 'coerce')
    df['salary'] = pd.to_numeric(df['salary'], errors = 'coerce')

    df['age'] = df['age'].fillna(df['age'].median())
    df['salary'] = df['salary'].fillna(df['salary'].median())

    return df

def clean_contact_fields(df):

    logging.info("Cleaning Email & Phone")

    df['email'] = df['email'].astype(str).str.lower().str.strip()
    df['phone'] = df['phone'].astype(str).str.replace(r'\D', '', regex = True)
    
    return df


def clean_date(df):

    logging.info("Cleaning Dates")

    df['signup_date'] = pd.to_datetime(df['signup_date'], errors = 'coerce')
    df['last_login'] = pd.to_datetime(df['last_login'], errors = 'coerce')

    return df


def remove_duplicates(df):

    logging.info("Removing duplicates")

    before  = len(df)
    df = df.drop_duplicates(subset = ['customer_id'])

    logging.info(f"Removed {before - len(df)} duplicate records")

    return df

def outlier_handling(df):

    logging.info("Outlier handling (IQR)")

    q1 = df['age'].quantile(0.25)
    q3 = df['age'].quantile(0.75)
    iqr = q3 - q1
    
    lower = q1 - 1.5*iqr
    upper = q3 + 1.5*iqr
    
    df.loc[:, 'age'] = df['age'].clip(lower, upper)

    return df

def silver_transformation(df):
    
    logging.info("Starting SILVER Layer processing")

    df = df.copy()
    
    df = clean_text_fields(df)
    df = clean_numeric_fields(df)
    df = clean_contact_fields(df)
    df = clean_date(df)
    df = remove_duplicates(df)
    df = outlier_handling(df)
    
    return df