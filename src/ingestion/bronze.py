import pandas as pd
from datetime import datetime
import logging
from pathlib import Path

logger = logging.getLogger("BRONZE")

def load_bronze(path: str) ->pd.DataFrame:

    logging.info(f"Reading Raw file: {path}")

    df = pd.read_csv(path)

    df.columns = (
        df.columns.str.strip().str.lower().str.replace(" ", "_")
    )

    df['ingestion_ts'] = datetime.utcnow()
    
    bronze_path = (
        f"data/bronze/bronze_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
    )

    df.to_parquet(bronze_path, index=False)
    
    Path("data/bronze").mkdir(parents = True, exist_ok = True)
    
    logger.info(f"Bronze snapshot saved → {bronze_path}")
    logging.info(f"Bronze loaded: rows={len(df)}, cols = {len(df.columns)}")

    return df