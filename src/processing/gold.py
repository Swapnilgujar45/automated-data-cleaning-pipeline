import pandas as pd
import logging

logger = logging.getLogger(__name__)

def gold_transformation(df):

    logging.info("Starting GOLD transformations")

    df['signup_year'] = df['signup_date'].dt.year
    df['signup_month'] = df['signup_date'].dt.month
    df['last_login_days_ago'] = (pd.Timestamp.now() - df['last_login']).dt.days

    df['salary_bucket'] = pd.cut(
        df['salary'],
        bins = [0, 300000, 700000, 1500000, float('inf')],
        labels = ['Low', 'Medium', 'High', 'Very High']
    )

    df['customer_segment'] = 'Regular'
    df.loc[df['salary']>700000, 'customer_segment'] = 'Premium'
    df.loc[df['salary']> 1200000, 'customer_segment'] = 'Elite'

    return df