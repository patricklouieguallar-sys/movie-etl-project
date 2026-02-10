# movie-etl-project
# Movie Data ETL Pipeline - Created Python script to process and clean movie data - Used pandas to filter high-rated movies and calculate profits - Loaded data into SQLite database

import pandas as pd
import sqlite3
import logging
import yaml

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)


def load_config(path: str) -> dict:
    """Load configuration from YAML file"""
    with open(path, "r") as file:
        return yaml.safe_load(file)


def extract(csv_path: str) -> pd.DataFrame:  # pd.DataFrame not pd.dataframe
    """Extract data from CSV file"""
    try:
        logging.info("Extracting data...")
        dataframe = pd.read_csv(csv_path)
        logging.info(f"Extracted {len(dataframe)} rows")
        return dataframe
    except Exception as e:
        logging.error(f"Extraction failed: {e}")
        raise


def transform(dataframe: pd.DataFrame, rating_threshold: float) -> pd.DataFrame:
    """Transform and clean the data"""
    try:
        logging.info("Transforming data...")

        # Remove rows with missing critical data
        dataframe = dataframe.dropna(
            subset=['title', 'vote_average', 'budget', 'revenue'])

        # Filter high-rated movies
        high_rated = dataframe[dataframe['vote_average']
                               > rating_threshold].copy()

        # Convert to millions
        high_rated['budget_millions'] = high_rated['budget'] / 1_000_000
        high_rated['revenue_millions'] = high_rated['revenue'] / 1_000_000
        high_rated['profit_millions'] = (
            high_rated['revenue_millions'] - high_rated['budget_millions']
        )

        logging.info(f"High-rated movies: {len(high_rated)}")
        return high_rated
    except Exception as e:
        logging.error(f"Transformation Failed: {e}")
        raise


def load(dataframe: pd.DataFrame, db_file: str, table_name: str):
    """Load data into SQLite database"""
    try:
        logging.info("Loading data...")
        with sqlite3.connect(db_file) as conn:
            dataframe.to_sql(table_name, conn,
                             if_exists='replace', index=False)
        logging.info("Load complete")
    except Exception as e:
        logging.error(f"Loading data failed: {e}")
        raise


def run_pipeline():
    """Main pipeline orchestrator"""
    try:
        logging.info("Pipeline starting to run")

        # Load configuration
        config = load_config("config.yaml")

        # Execute ETL steps
        df = extract(config["input_csv"])
        transformed_df = transform(
            df,
            rating_threshold=config["transform"]["rating_threshold"]
        )
        load(
            transformed_df,
            config["database"]["file"],
            config["database"]["table"]
        )

        logging.info("Pipeline completed successfully!")

    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    run_pipeline()
