"""
Train traffic speed prediction model.
Same logic as the notebook, but runs as a standalone script.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pandas as pd
import numpy as np
from sqlalchemy import text
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
from sklearn.preprocessing import LabelEncoder
import joblib

from src.database.connection import engine
from src.config.settings import settings


def train():
    """Full training pipeline."""
    
    print("\n" + "=" * 60)
    print("🤖 TRAFFIC SPEED MODEL TRAINING")
    print("=" * 60)
    
    # ================================================================
    # 1. LOAD DATA
    # ================================================================
    print("\n📦 Step 1: Loading data from PostGIS...")
    
    traffic_df = pd.read_sql(
        "SELECT * FROM traffic_readings ORDER BY timestamp",
        engine
    )
    traffic_df["timestamp"] = pd.to_datetime(traffic_df["timestamp"])
    
    weather_df = pd.read_sql(
        "SELECT * FROM weather_readings ORDER BY timestamp",
        engine
    )
    weather_df["timestamp"] = pd.to_datetime(weather_df["timestamp"])
    
    print(f"  Traffic readings: {len(traffic_df):,}")
    print(f"  Weather readings: {len(weather_df):,}")
    print(f"  Time range: {traffic_df['timestamp'].min()} → {traffic_df['timestamp'].max()}")
    print(f"  Locations: {traffic_df['location_name'].nunique()}")
    
    if len(traffic_df) < 20:
        print("\n⚠️  Not enough traffic data for training (need at least 20 rows).")
        print("   Let the scheduler run longer and try again.")
        return
    
    # ================================================================
    # 2. FEATURE ENGINEERING
    # ================================================================
    print("\n🔧 Step 2: Feature engineering...")
    
    df = traffic_df.copy()
    
    # Time features
    df["hour"] = df["timestamp"].dt.hour
    df["day_of_week"] = df["timestamp"].dt.dayofweek
    df["is_weekend"] = (df["day_of_week"] >= 5).astype(int)
    df["is_rush_hour"] = (
        ((df["hour"] >= 8) & (df["hour"] < 10)) |
        ((df["hour"] >= 17) & (df["hour"] < 20))
    ).astype(int)
    
    # Cyclical encoding
    df["hour_sin"] = np.sin(2 * np.pi * df["hour"] / 24)
    df["hour_cos"] = np.cos(2 * np.pi * df["hour"] / 24)
    df["dow_sin"] = np.sin(2 * np.pi * df["day_of_week"] / 7)
    df["dow_cos"] = np.cos(2 * np.pi * df["day_of_week"] / 7)
    
    print(f"  Time features added")
    print(f"  Rush hour readings: {df['is_rush_hour'].sum()}")
    print(f"  Weekend readings: {df['is_weekend'].sum()}")
    
    # Location encoding
    le_location = LabelEncoder()
    df["location_encoded"] = le_location.fit_transform(df["location_name"])
    
    print(f"\n  Location encoding:")
    for i, name in enumerate(le_location.classes_):
        count = (df["location_encoded"] == i).sum()
        print(f"    {i} → {name} ({count} readings)")
    
    # Weather merge
    if len(weather_df) > 0:
        weather_features = weather_df[["timestamp", "temperature", "humidity",
                                        "wind_speed", "rain_1h", "visibility"]].copy()
        weather_features = weather_features.sort_values("timestamp")
        
        df = df.sort_values("timestamp")
        df = pd.merge_asof(
            df,
            weather_features,
            on="timestamp",
            direction="nearest",
            tolerance=pd.Timedelta("2h"),
        )
        
        df["temperature"] = df["temperature"].fillna(25.0)
        df["humidity"] = df["humidity"].fillna(60.0)
        df["wind_speed"] = df["wind_speed"].fillna(2.0)
        df["rain_1h"] = df["rain_1h"].fillna(0.0)
        df["visibility"] = df["visibility"].fillna(10000.0)
        df["is_raining"] = (df["rain_1h"] > 0).astype(int)
        
        print(f"\n  Weather features merged")
        print(f"  Raining readings: {df['is_raining'].sum()}")
    else:
        df["temperature"] = 25.0
        df["humidity"] = 60.0
        df["wind_speed"] = 2.0
        df["rain_1h"] = 0.0
        df["visibility"] = 10000.0
        df["is_raining"] = 0
        print("\n  ⚠️ No weather data — using defaults")
    
    # Lag features
    df = df.sort_values(["location_name", "timestamp"]).reset_index(drop=True)
    
    for lag_name, lag_periods in [("lag_1", 1), ("lag_3", 3), ("lag_6", 6)]:
        df[f"speed_{lag_name}"] = df.groupby("location_name")["current_speed"].shift(lag_periods)
        df[f"ratio_{lag_name}"] = df.groupby("location_name")["congestion_ratio"].shift(lag_periods)
    
    df["speed_rolling_3"] = df.groupby("location_name")["current_speed"].transform(
        lambda x: x.rolling(3, min_periods=1).mean()
    )
    df["speed_rolling_6"] = df.groupby("location_name")["current_speed"].transform(
        lambda x: x.rolling(6, min_periods=1).mean()
    )
    
    before = len(df)
    df = df.dropna(subset=["speed_lag_1", "speed_lag_3", "speed_lag_6"])
    after = len(df)
    
    print(f"\n  Lag features added")
    print(f"  Dropped {before - after} rows with NaN lags")
    print(f"  Final dataset: {len(df):,} rows")
    
    if len(df) < 16:
        print("\n⚠️  Not enough data after lag features. Need more collection time.")
        print("   Let the scheduler run for at least 1 hour and try again.")
        return
    
    # ================================================================
    # 3. TRAIN/TEST SPLIT
    # ================================================================
    print("\n📊 Step 3: Train/test split...")
    
    feature_cols = [
        "hour", "day_of_week", "is_weekend", "is_rush_hour",
        "hour_sin", "hour_cos", "dow_sin", "dow_cos",
        "location_encoded",
        "free_flow_speed", "confidence",
        "temperature", "humidity", "wind_speed", "rain_1h", "visibility", "is_raining",
        "speed_lag_1", "speed_lag_3", "speed_lag_6",
        "ratio_lag_1", "ratio_lag_3", "ratio_lag_6",
        "speed_rolling_3", "speed_rolling_6",
    ]
    
    target_col = "current_speed"
    
    df = df.sort_values("timestamp").reset_index(drop=True)
    
    X = df[feature_cols]
    y = df[target_col]
    
    split_idx = int(len(df) * 0.8)
    
    # Ensure at least 1 sample in each set
    if split_idx < 1:
        split_idx = 1
    if split_idx >= len(df):
        split_idx = len(df) - 1
    
    X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
    y_train, y_test = y.iloc[:split_idx], y.iloc[split_idx:]
    
    print(f"  Features: {len(feature_cols)}")
    print(f"  Train: {len(X_train):,} rows")
    print(f"  Test:  {len(X_test):,} rows")
    print(f"  Target mean: {y_train.mean():.1f} km/h")
    print(f"  Target std:  {y_train.std():.1f} km/h")
    
    # ================================================================
    # 4. TRAIN RANDOM FOREST
    # ================================================================
    print("\n🌲 Step 4: Training Random Forest...")
    
    rf_model = RandomForestRegressor(
        n_estimators=100,
        max_depth=15,
        min_samples_split=5,
        min_samples_leaf=2,
        random_state=42,
        n_jobs=-1,
    )
    
    rf_model.fit(X_train, y_train)
    
    rf_train_pred = rf_model.predict(X_train)
    rf_test_pred = rf_model.predict(X_test)
    
    rf_train_mae = mean_absolute_error(y_train, rf_train_pred)
    rf_test_mae = mean_absolute_error(y_test, rf_test_pred)
    rf_train_r2 = r2_score(y_train, rf_train_pred)
    rf_test_r2 = r2_score(y_test, rf_test_pred)
    rf_test_rmse = np.sqrt(mean_squared_error(y_test, rf_test_pred))
    
    print(f"  TRAIN — MAE: {rf_train_mae:.2f} | R²: {rf_train_r2:.4f}")
    print(f"  TEST  — MAE: {rf_test_mae:.2f} | RMSE: {rf_test_rmse:.2f} | R²: {rf_test_r2:.4f}")
    
    # ================================================================
    # 5. TRAIN GRADIENT BOOSTING
    # ================================================================
    print("\n🚀 Step 5: Training Gradient Boosting...")
    
    gb_model = GradientBoostingRegressor(
        n_estimators=200,
        max_depth=6,
        learning_rate=0.1,
        min_samples_split=5,
        min_samples_leaf=3,
        subsample=0.8,
        random_state=42,
    )
    
    gb_model.fit(X_train, y_train)
    
    gb_train_pred = gb_model.predict(X_train)
    gb_test_pred = gb_model.predict(X_test)
    
    gb_train_mae = mean_absolute_error(y_train, gb_train_pred)
    gb_test_mae = mean_absolute_error(y_test, gb_test_pred)
    gb_train_r2 = r2_score(y_train, gb_train_pred)
    gb_test_r2 = r2_score(y_test, gb_test_pred)
    gb_test_rmse = np.sqrt(mean_squared_error(y_test, gb_test_pred))
    
    print(f"  TRAIN — MAE: {gb_train_mae:.2f} | R²: {gb_train_r2:.4f}")
    print(f"  TEST  — MAE: {gb_test_mae:.2f} | RMSE: {gb_test_rmse:.2f} | R²: {gb_test_r2:.4f}")
    
    # ================================================================
    # 6. COMPARE & SELECT BEST
    # ================================================================
    print(f"\n{'='*60}")
    print("📋 MODEL COMPARISON (Test Set)")
    print(f"{'='*60}")
    print(f"  {'Model':<25s} {'MAE':>8s} {'RMSE':>8s} {'R²':>10s}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*10}")
    print(f"  {'Random Forest':<25s} {rf_test_mae:>8.2f} {rf_test_rmse:>8.2f} {rf_test_r2:>10.4f}")
    print(f"  {'Gradient Boosting':<25s} {gb_test_mae:>8.2f} {gb_test_rmse:>8.2f} {gb_test_r2:>10.4f}")
    
    if gb_test_r2 > rf_test_r2:
        best_model = gb_model
        best_name = "GradientBoosting"
        best_pred = gb_test_pred
        best_mae = gb_test_mae
        best_rmse = gb_test_rmse
        best_r2 = gb_test_r2
    else:
        best_model = rf_model
        best_name = "RandomForest"
        best_pred = rf_test_pred
        best_mae = rf_test_mae
        best_rmse = rf_test_rmse
        best_r2 = rf_test_r2
    
    print(f"\n  🏆 Best model: {best_name}")
    
    # ================================================================
    # 7. FEATURE IMPORTANCES
    # ================================================================
    print(f"\n{'='*60}")
    print("🌟 FEATURE IMPORTANCES")
    print(f"{'='*60}")
    
    importances = best_model.feature_importances_
    feat_imp = sorted(zip(feature_cols, importances), key=lambda x: x[1], reverse=True)
    
    for feat, imp in feat_imp[:10]:
        bar = "█" * int(imp * 100)
        print(f"  {feat:<25s} {imp:.4f} {bar}")
    
    # ================================================================
    # 8. SAVE MODEL
    # ================================================================
    print(f"\n{'='*60}")
    print("💾 SAVING MODEL")
    print(f"{'='*60}")
    
    models_dir = settings.PROJECT_ROOT / "models"
    models_dir.mkdir(exist_ok=True)
    
    # Save model
    model_path = models_dir / "traffic_speed_model.joblib"
    joblib.dump(best_model, model_path)
    print(f"  ✅ Model saved: {model_path}")
    
    # Save encoder
    le_path = models_dir / "location_encoder.joblib"
    joblib.dump(le_location, le_path)
    print(f"  ✅ Encoder saved: {le_path}")
    
    # Save config
    config_path = models_dir / "model_config.joblib"
    model_config = {
        "feature_cols": feature_cols,
        "target_col": target_col,
        "model_name": best_name,
        "train_size": len(X_train),
        "test_size": len(X_test),
        "test_mae": best_mae,
        "test_rmse": best_rmse,
        "test_r2": best_r2,
        "location_classes": list(le_location.classes_),
        "trained_at": str(pd.Timestamp.now()),
    }
    joblib.dump(model_config, config_path)
    print(f"  ✅ Config saved: {config_path}")
    
    # ================================================================
    # 9. SUMMARY
    # ================================================================
    print(f"\n{'='*60}")
    print("📋 TRAINING COMPLETE")
    print(f"{'='*60}")
    print(f"  Model:      {best_name}")
    print(f"  Train size: {len(X_train):,}")
    print(f"  Test size:  {len(X_test):,}")
    print(f"  Test MAE:   {best_mae:.2f} km/h")
    print(f"  Test RMSE:  {best_rmse:.2f} km/h")
    print(f"  Test R²:    {best_r2:.4f}")
    print(f"  Features:   {len(feature_cols)}")
    print(f"  Saved to:   {models_dir}")
    print(f"{'='*60}")


if __name__ == "__main__":
    train()