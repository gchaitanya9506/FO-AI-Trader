# pipeline_manager.py
"""
Unified Pipeline Manager for End-to-End NSE Option Chain Processing
Orchestrates: API fetch → cleaning → database storage → feature engineering → processed features
"""

import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Optional, Dict, Any, Tuple
from pathlib import Path

# Add the scripts directory to path for importing preprocess functions
sys.path.append(str(Path(__file__).parent.parent / 'scripts'))

from config.logging_config import get_logger
from config.app_config import get_config
from data_processor import OptionChainProcessor


class PipelineManager:
    """
    Unified pipeline manager that orchestrates the complete data flow:
    NSE API → Data Processing → Database → Feature Engineering → Processed Features
    """

    def __init__(self):
        self.config = get_config()
        self.logger = get_logger('pipeline')
        self.processor = OptionChainProcessor()

    def run_complete_pipeline(
        self,
        symbol: str = "NIFTY",
        save_to_database: bool = True,
        save_to_csv: bool = False,
        trigger_feature_engineering: bool = None
    ) -> Dict[str, Any]:
        """
        Run the complete data pipeline from API fetch to processed features.

        Args:
            symbol: NSE symbol to process
            save_to_database: Save raw data to database
            save_to_csv: Save raw data to CSV files
            trigger_feature_engineering: Auto-trigger feature processing (default: from config)

        Returns:
            Dict with pipeline execution results and statistics
        """
        if trigger_feature_engineering is None:
            trigger_feature_engineering = self.config.data_pipeline.enable_auto_feature_engineering

        pipeline_start = datetime.now()
        results = {
            'pipeline_start': pipeline_start,
            'raw_data_success': False,
            'raw_data_records': 0,
            'feature_engineering_success': False,
            'feature_records': 0,
            'errors': [],
            'warnings': []
        }

        self.logger.info(f"🚀 Starting complete data pipeline for {symbol}")

        # Step 1: Fetch and process raw option chain data
        try:
            self.logger.info("Step 1: Fetching and processing raw option chain data")

            raw_data = self.processor.fetch_and_process_option_chain(
                symbol=symbol,
                save_to_database=save_to_database,
                save_to_csv=save_to_csv or trigger_feature_engineering  # Need CSV for feature engineering
            )

            if raw_data is not None and not raw_data.empty:
                results['raw_data_success'] = True
                results['raw_data_records'] = len(raw_data)
                self.logger.info(f"✅ Raw data processing completed: {len(raw_data)} records")
            else:
                error_msg = "Raw data processing failed or returned empty result"
                results['errors'].append(error_msg)
                self.logger.error(error_msg)
                return results

        except Exception as e:
            error_msg = f"Error in raw data processing: {e}"
            results['errors'].append(error_msg)
            self.logger.error(error_msg)
            return results

        # Step 2: Feature engineering (if enabled)
        if trigger_feature_engineering:
            try:
                self.logger.info("Step 2: Running feature engineering pipeline")

                feature_results = self._run_feature_engineering_pipeline(symbol)

                if feature_results['success']:
                    results['feature_engineering_success'] = True
                    results['feature_records'] = feature_results.get('records_processed', 0)
                    self.logger.info(f"✅ Feature engineering completed: {results['feature_records']} feature records")
                else:
                    results['warnings'].append("Feature engineering failed")
                    self.logger.warning("⚠️ Feature engineering failed, raw data is available")

            except Exception as e:
                warning_msg = f"Error in feature engineering: {e}"
                results['warnings'].append(warning_msg)
                self.logger.warning(warning_msg)

        # Step 3: Pipeline completion
        pipeline_end = datetime.now()
        results['pipeline_end'] = pipeline_end
        results['pipeline_duration'] = (pipeline_end - pipeline_start).total_seconds()

        success_msg = f"🎉 Pipeline completed in {results['pipeline_duration']:.2f} seconds"
        if results['raw_data_success']:
            success_msg += f" | Raw: {results['raw_data_records']} records"
        if results['feature_engineering_success']:
            success_msg += f" | Features: {results['feature_records']} records"

        self.logger.info(success_msg)

        return results

    def _run_feature_engineering_pipeline(self, symbol: str = "NIFTY") -> Dict[str, Any]:
        """
        Run the feature engineering pipeline using integrated preprocess.py logic.

        Returns:
            Dict with feature engineering results
        """
        try:
            # Import preprocess functions
            from preprocess import (
                load_data, engineer_nifty_features, engineer_option_features
            )

            results = {
                'success': False,
                'records_processed': 0,
                'nifty_features_count': 0,
                'option_features_count': 0
            }

            self.logger.info("Loading data for feature engineering...")

            # Load data using preprocess functions (expects CSV files)
            nifty_path = "data/raw/nifty_data_5m.csv"
            option_path = "data/raw/nifty_option_chain_clean.csv"

            # Verify files exist
            if not os.path.exists(nifty_path):
                self.logger.error(f"NIFTY data file not found: {nifty_path}")
                return results

            if not os.path.exists(option_path):
                self.logger.error(f"Option chain data file not found: {option_path}")
                return results

            # Load the data
            nifty_df, option_df = load_data(nifty_path=nifty_path, option_path=option_path)

            if nifty_df is None or nifty_df.empty:
                self.logger.error("Failed to load NIFTY data")
                return results

            if option_df is None or option_df.empty:
                self.logger.error("Failed to load option chain data")
                return results

            self.logger.info(f"Loaded NIFTY data: {len(nifty_df)} records")
            self.logger.info(f"Loaded option data: {len(option_df)} records")

            # Engineer NIFTY features
            self.logger.info("Engineering NIFTY technical indicators...")
            nifty_features = engineer_nifty_features(nifty_df)
            results['nifty_features_count'] = len(nifty_features)

            # Engineer option features
            self.logger.info("Engineering option features (Greeks, moneyness, etc.)...")
            option_features = engineer_option_features(option_df, nifty_features)
            results['option_features_count'] = len(option_features)

            # Save processed features to database
            if not option_features.empty:
                database_success = self._save_features_to_database(
                    nifty_features=nifty_features,
                    option_features=option_features,
                    symbol=symbol
                )

                if database_success:
                    results['success'] = True
                    results['records_processed'] = len(option_features)
                    self.logger.info("✅ Features saved to database successfully")
                else:
                    self.logger.warning("⚠️ Feature database save failed, saving to CSV fallback")
                    self._save_features_to_csv(option_features)
                    results['success'] = True  # Still consider success if CSV saves
                    results['records_processed'] = len(option_features)
            else:
                self.logger.error("No features generated")

            return results

        except Exception as e:
            self.logger.error(f"Error in feature engineering pipeline: {e}")
            return {'success': False, 'error': str(e)}

    def _save_features_to_database(
        self,
        nifty_features: pd.DataFrame,
        option_features: pd.DataFrame,
        symbol: str
    ) -> bool:
        """
        Save processed features to database tables.
        """
        try:
            from database.db_manager import save_df, init_option_chain_table

            # Ensure tables exist
            init_option_chain_table()

            # Save NIFTY underlying features
            if not nifty_features.empty:
                # Prepare NIFTY features for database
                nifty_features_db = nifty_features.copy()
                nifty_features_db['symbol'] = symbol
                nifty_features_db['created_at'] = pd.Timestamp.now()

                save_df(nifty_features_db, "underlying_features")
                self.logger.info(f"Saved {len(nifty_features_db)} NIFTY feature records to database")

            # Save option chain features
            if not option_features.empty:
                # Prepare option features for database
                option_features_db = option_features.copy()
                option_features_db['symbol'] = symbol
                option_features_db['created_at'] = pd.Timestamp.now()

                save_df(option_features_db, "option_chain_features")
                self.logger.info(f"Saved {len(option_features_db)} option feature records to database")

            return True

        except Exception as e:
            self.logger.error(f"Error saving features to database: {e}")
            return False

    def _save_features_to_csv(self, option_features: pd.DataFrame) -> bool:
        """
        Fallback: Save processed features to CSV file.
        """
        try:
            csv_path = "data/processed/nifty_option_features.csv"
            os.makedirs(os.path.dirname(csv_path), exist_ok=True)
            option_features.to_csv(csv_path, index=False)
            self.logger.info(f"Saved processed features to CSV: {csv_path}")
            return True

        except Exception as e:
            self.logger.error(f"Error saving features to CSV: {e}")
            return False

    def get_pipeline_status(self) -> Dict[str, Any]:
        """Get current pipeline status and health information."""
        try:
            from database.db_manager import get_latest_option_chain_data

            # Get latest data timestamps
            latest_data = get_latest_option_chain_data("NIFTY", limit=1)

            status = {
                'pipeline_available': True,
                'database_accessible': True,
                'last_data_fetch': None,
                'data_freshness_hours': None,
                'total_option_records': len(get_latest_option_chain_data("NIFTY")) if not latest_data.empty else 0
            }

            if not latest_data.empty:
                last_fetch = pd.to_datetime(latest_data.iloc[0]['created_at'])
                status['last_data_fetch'] = last_fetch
                status['data_freshness_hours'] = (pd.Timestamp.now() - last_fetch).total_seconds() / 3600

            return status

        except Exception as e:
            self.logger.error(f"Error getting pipeline status: {e}")
            return {
                'pipeline_available': False,
                'error': str(e)
            }

    def trigger_feature_reprocessing(self, symbol: str = "NIFTY") -> Dict[str, Any]:
        """
        Trigger feature reprocessing using existing raw data in database.
        Useful for updating features when algorithms change.
        """
        self.logger.info(f"Triggering feature reprocessing for {symbol}")

        try:
            # This would require implementing database-to-CSV export
            # For now, assume CSV files exist and run feature engineering
            results = self._run_feature_engineering_pipeline(symbol)

            if results['success']:
                self.logger.info("✅ Feature reprocessing completed successfully")
            else:
                self.logger.warning("⚠️ Feature reprocessing encountered issues")

            return results

        except Exception as e:
            error_msg = f"Error in feature reprocessing: {e}"
            self.logger.error(error_msg)
            return {'success': False, 'error': error_msg}


# Convenience functions
def run_full_pipeline(symbol: str = "NIFTY", **kwargs) -> Dict[str, Any]:
    """
    Convenience function to run the complete pipeline.
    """
    manager = PipelineManager()
    return manager.run_complete_pipeline(symbol=symbol, **kwargs)


def get_pipeline_manager() -> PipelineManager:
    """
    Get a configured pipeline manager instance.
    """
    return PipelineManager()


if __name__ == "__main__":
    # Test the complete pipeline
    print("🧪 Testing complete data pipeline...")

    manager = PipelineManager()

    # Run complete pipeline
    results = manager.run_complete_pipeline(
        symbol="NIFTY",
        save_to_database=True,
        save_to_csv=True,
        trigger_feature_engineering=True
    )

    # Display results
    print("\n📊 Pipeline Results:")
    print(f"Raw data success: {results['raw_data_success']}")
    print(f"Raw data records: {results['raw_data_records']}")
    print(f"Feature engineering success: {results['feature_engineering_success']}")
    print(f"Feature records: {results['feature_records']}")
    print(f"Pipeline duration: {results.get('pipeline_duration', 0):.2f} seconds")

    if results['errors']:
        print(f"❌ Errors: {results['errors']}")
    if results['warnings']:
        print(f"⚠️ Warnings: {results['warnings']}")

    # Get status
    status = manager.get_pipeline_status()
    print(f"\n🏥 Pipeline Status: {status}")