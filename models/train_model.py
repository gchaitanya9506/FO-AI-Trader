import pandas as pd
import xgboost as xgb
import joblib
import os
from datetime import datetime
from sklearn.metrics import classification_report, confusion_matrix, roc_auc_score, accuracy_score
from database.db_manager import load_table
from config.logging_config import get_logger

logger = get_logger('models')


def create_time_series_splits(df, train_ratio=0.7, val_ratio=0.15, test_ratio=0.15):
    """
    Create time series splits that respect temporal order.
    No look-ahead bias: train < validation < test chronologically.
    """
    if abs(train_ratio + val_ratio + test_ratio - 1.0) > 1e-6:
        raise ValueError("Split ratios must sum to 1.0")

    # Sort by datetime to ensure chronological order
    df_sorted = df.sort_values("Datetime").reset_index(drop=True)
    n_samples = len(df_sorted)

    train_end = int(n_samples * train_ratio)
    val_end = int(n_samples * (train_ratio + val_ratio))

    train_data = df_sorted.iloc[:train_end].copy()
    val_data = df_sorted.iloc[train_end:val_end].copy()
    test_data = df_sorted.iloc[val_end:].copy()

    logger.info(f"Time series splits created:")
    logger.info(f"  Train: {len(train_data)} samples ({train_data['Datetime'].min()} to {train_data['Datetime'].max()})")
    logger.info(f"  Validation: {len(val_data)} samples ({val_data['Datetime'].min()} to {val_data['Datetime'].max()})")
    logger.info(f"  Test: {len(test_data)} samples ({test_data['Datetime'].min()} to {test_data['Datetime'].max()})")

    return train_data, val_data, test_data


def create_target_variable(df, prediction_horizon=1):
    """
    Create target variable without look-ahead bias.

    Args:
        df: DataFrame with OHLC data, sorted by Datetime
        prediction_horizon: Number of periods to predict ahead

    Returns:
        DataFrame with target variable
    """
    df_copy = df.copy()

    # Create future price (shift by negative value to get future data)
    # But we'll only use this for samples where we actually have future data
    df_copy["future_close"] = df_copy["Close"].shift(-prediction_horizon)

    # Create target: 1 if price goes up, 0 if down or stays same
    df_copy["target"] = (df_copy["future_close"] > df_copy["Close"]).astype(int)

    # Remove samples where we don't have future data (end of dataset)
    # This prevents look-ahead bias
    df_copy = df_copy.dropna(subset=["future_close"]).drop(columns=["future_close"])

    logger.info(f"Target variable created with {prediction_horizon} period horizon")
    logger.info(f"Samples with valid targets: {len(df_copy)}")
    logger.info(f"Target distribution: {df_copy['target'].value_counts().to_dict()}")

    return df_copy


def evaluate_model(model, X_test, y_test, model_name="Model"):
    """Evaluate model performance with comprehensive metrics."""
    y_pred = model.predict(X_test)
    y_pred_proba = model.predict_proba(X_test)[:, 1]

    # Calculate metrics
    accuracy = accuracy_score(y_test, y_pred)
    auc_score = roc_auc_score(y_test, y_pred_proba)

    logger.info(f"\n{model_name} Performance Metrics:")
    logger.info(f"Accuracy: {accuracy:.4f}")
    logger.info(f"AUC-ROC: {auc_score:.4f}")

    # Classification report
    logger.info(f"Classification Report:\n{classification_report(y_test, y_pred)}")

    # Confusion Matrix
    logger.info(f"Confusion Matrix:\n{confusion_matrix(y_test, y_pred)}")

    return {
        'accuracy': accuracy,
        'auc_roc': auc_score,
        'predictions': y_pred,
        'prediction_probabilities': y_pred_proba
    }


def train_model_with_validation():
    """Train model with proper time series validation and no data leakage."""

    logger.info("Starting model training with time series validation...")

    # Load data
    try:
        df = load_table("underlying_features")
        logger.info(f"Loaded {len(df)} samples from underlying_features table")
    except Exception as e:
        logger.error(f"Failed to load data: {e}")
        raise

    # Ensure datetime column exists and is properly formatted
    if "Datetime" not in df.columns:
        logger.error("Datetime column not found in data")
        raise ValueError("Datetime column required for time series modeling")

    df["Datetime"] = pd.to_datetime(df["Datetime"])

    # Create target variable without look-ahead bias
    df_with_target = create_target_variable(df, prediction_horizon=1)

    # Define features
    features = ["ema9", "ema21", "rsi", "atr", "vwap"]

    # Check if all features are available
    missing_features = [f for f in features if f not in df_with_target.columns]
    if missing_features:
        logger.error(f"Missing features: {missing_features}")
        raise ValueError(f"Missing required features: {missing_features}")

    # Remove rows with NaN values in features or target
    df_clean = df_with_target.dropna(subset=features + ["target"])
    logger.info(f"After removing NaN values: {len(df_clean)} samples remain")

    if len(df_clean) < 100:
        logger.warning("Very small dataset - model may not be reliable")

    # Create time series splits
    train_df, val_df, test_df = create_time_series_splits(df_clean)

    # Prepare training data
    X_train = train_df[features]
    y_train = train_df["target"]

    X_val = val_df[features]
    y_val = val_df["target"]

    X_test = test_df[features]
    y_test = test_df["target"]

    # Train model with validation
    logger.info("Training XGBoost model...")

    model = xgb.XGBClassifier(
        n_estimators=200,
        max_depth=5,
        learning_rate=0.1,
        random_state=42,
        eval_metric='logloss'
    )

    # Fit with early stopping on validation set
    model.fit(
        X_train, y_train,
        eval_set=[(X_val, y_val)],
        early_stopping_rounds=20,
        verbose=False
    )

    # Evaluate on validation set
    logger.info("Evaluating on validation set:")
    val_metrics = evaluate_model(model, X_val, y_val, "Validation")

    # Evaluate on test set
    logger.info("Evaluating on test set:")
    test_metrics = evaluate_model(model, X_test, y_test, "Test")

    # Feature importance
    feature_importance = dict(zip(features, model.feature_importances_))
    logger.info(f"Feature Importance: {feature_importance}")

    # Save model with metadata
    model_dir = "models/saved_models"
    os.makedirs(model_dir, exist_ok=True)

    model_path = os.path.join(model_dir, "nifty_direction.model")
    joblib.dump(model, model_path)

    # Save model metadata
    metadata = {
        'training_date': datetime.now().isoformat(),
        'features': features,
        'train_samples': len(train_df),
        'val_samples': len(val_df),
        'test_samples': len(test_df),
        'val_accuracy': val_metrics['accuracy'],
        'val_auc': val_metrics['auc_roc'],
        'test_accuracy': test_metrics['accuracy'],
        'test_auc': test_metrics['auc_roc'],
        'feature_importance': feature_importance,
        'model_params': model.get_params()
    }

    metadata_path = os.path.join(model_dir, "nifty_direction_metadata.json")
    import json
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    logger.info(f"Model saved to {model_path}")
    logger.info(f"Model metadata saved to {metadata_path}")
    logger.info("Model training completed successfully!")

    return model, metadata


if __name__ == "__main__":
    try:
        model, metadata = train_model_with_validation()
    except Exception as e:
        logger.error(f"Model training failed: {e}")
        raise