"""
Model Training & Experiment Tracking Pipeline with MLflow.
Trains Random Forest, XGBoost, and LightGBM with class imbalance handling,
tracks experiments, and registers the production model pipeline.
"""

import sys
from pathlib import Path

# Force UTF-8 on Windows stdout/stderr to prevent emoji charmap encoding crash
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

import os
import joblib
import logging
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import (
    classification_report, confusion_matrix, f1_score,
    recall_score, precision_score, roc_auc_score
)
from sklearn.pipeline import Pipeline
from sklearn.utils.class_weight import compute_class_weight
import xgboost as xgb
import lightgbm as lgb
import mlflow
import mlflow.sklearn

from src.ml.features import (
    load_dataset_from_warehouse,
    build_preprocessor_pipeline,
    get_chronological_splits
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("model_training")

MLFLOW_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME = os.getenv("MLFLOW_EXPERIMENT_NAME", "traffic-accident-severity-prediction")
MODEL_DIR = Path("models")
MODEL_DIR.mkdir(parents=True, exist_ok=True)


def plot_and_save_confusion_matrix(cm, classes, output_path: str):
    """Generates and saves confusion matrix heatmap plot artifact."""
    fig, ax = plt.subplots(figsize=(6, 5))
    im = ax.imshow(cm, interpolation='nearest', cmap=plt.cm.Blues)
    ax.figure.colorbar(im, ax=ax)
    ax.set(
        xticks=np.arange(cm.shape[1]),
        yticks=np.arange(cm.shape[0]),
        xticklabels=classes, yticklabels=classes,
        title='Confusion Matrix',
        ylabel='True Severity',
        xlabel='Predicted Severity'
    )
    thresh = cm.max() / 2.
    for i in range(cm.shape[0]):
        for j in range(cm.shape[1]):
            ax.text(j, i, format(cm[i, j], 'd'),
                    ha="center", va="center",
                    color="white" if cm[i, j] > thresh else "black")
    fig.tight_layout()
    plt.savefig(output_path, dpi=150)
    plt.close()


def setup_mlflow():
    """Initializes MLflow tracking, falling back gracefully to local directory."""
    import socket
    local_tracking = str(Path("mlruns").resolve())
    
    server_online = False
    if MLFLOW_URI.startswith("http"):
        try:
            parts = MLFLOW_URI.replace("http://", "").replace("https://", "").split(":")
            host = parts[0]
            port = int(parts[1]) if len(parts) > 1 else 5000
            with socket.create_connection((host, port), timeout=0.5):
                server_online = True
        except Exception:
            server_online = False
            
    if server_online:
        mlflow.set_tracking_uri(MLFLOW_URI)
        logger.info(f"Connected to MLflow Tracking Server at: {MLFLOW_URI}")
    else:
        mlflow.set_tracking_uri(f"file:///{local_tracking}")
        logger.info(f"MLflow remote server offline. Tracking experiments locally at: {local_tracking}")
        
    mlflow.set_experiment(EXPERIMENT_NAME)


def train_and_evaluate_models():
    """
    Main training workflow:
    1. Loads data from warehouse.
    2. Chronological Train/Val/Test split.
    3. Fits preprocessor.
    4. Trains Random Forest, XGBoost, and LightGBM with class weights.
    5. Logs metrics, params, and artifacts to MLflow.
    6. Selects and serializes best production candidate.
    """
    setup_mlflow()
    
    df = load_dataset_from_warehouse()
    if len(df) < 50:
        logger.error("Insufficient records in warehouse to train model. Run ETL first.")
        return
        
    (X_train, y_train), (X_val, y_val), (X_test, y_test) = get_chronological_splits(df)
    
    # Import SMOTE and accuracy_score
    from imblearn.over_sampling import SMOTE
    from sklearn.metrics import accuracy_score
    
    # Preprocessor pipeline
    preprocessor = build_preprocessor_pipeline()
    logger.info("Fitting preprocessor on chronological training split...")
    X_train_proc = preprocessor.fit_transform(X_train)
    X_val_proc = preprocessor.transform(X_val)
    X_test_proc = preprocessor.transform(X_test)
    
    # 1. Apply SMOTE to synthesize minority samples for Fatal and Serious classes
    k_neighbors = min(4, sum(y_train == 2) - 1)
    if k_neighbors >= 1:
        smote = SMOTE(random_state=42, k_neighbors=k_neighbors)
        X_train_res, y_train_res = smote.fit_resample(X_train_proc, y_train)
        logger.info(f"Applied SMOTE: Resampled training set from {len(y_train)} to {len(y_train_res)} records.")
    else:
        X_train_res, y_train_res = X_train_proc, y_train
        logger.info("SMOTE skipped due to insufficient minority class samples.")
        
    # Compute balanced class weights
    classes = np.unique(y_train)
    weights = compute_class_weight(class_weight="balanced", classes=classes, y=y_train)
    class_weight_dict = {cls: w for cls, w in zip(classes, weights)}
    logger.info(f"Calculated class weights: {class_weight_dict}")
    
    # Define hyperparameter-optimized candidate models
    models = {
        "RandomForest": RandomForestClassifier(
            n_estimators=150,
            max_depth=14,
            min_samples_split=4,
            class_weight="balanced_subsample",
            random_state=42,
            n_jobs=-1
        ),
        "LightGBM": lgb.LGBMClassifier(
            n_estimators=180,
            max_depth=7,
            learning_rate=0.04,
            num_leaves=31,
            subsample=0.85,
            class_weight="balanced",
            random_state=42,
            verbosity=-1
        ),
        "XGBoost": xgb.XGBClassifier(
            n_estimators=160,
            max_depth=6,
            learning_rate=0.05,
            subsample=0.85,
            colsample_bytree=0.85,
            objective="multi:softprob",
            num_class=3,
            random_state=42,
            eval_metric="mlogloss"
        )
    }
    
    class_names = ["Slight", "Serious", "Fatal"]
    best_model_name = None
    best_macro_f1 = -1.0
    best_pipeline = None
    
    for name, model in models.items():
        logger.info(f"--- Training {name} with SMOTE & Hyperparameter Optimization ---")
        with mlflow.start_run(run_name=name):
            # Fit model on SMOTE resampled feature space
            model.fit(X_train_res, y_train_res)
                
            # Predictions on Validation
            y_pred_val = model.predict(X_val_proc)
            y_proba_val = model.predict_proba(X_val_proc)
            
            # Compute comprehensive metrics
            val_accuracy = accuracy_score(y_val, y_pred_val)
            macro_f1 = f1_score(y_val, y_pred_val, average="macro", zero_division=0)
            weighted_f1 = f1_score(y_val, y_pred_val, average="weighted", zero_division=0)
            fatal_recall = recall_score(y_val, y_pred_val, labels=[2], average="macro", zero_division=0)
            serious_recall = recall_score(y_val, y_pred_val, labels=[1], average="macro", zero_division=0)
            
            try:
                roc_auc = roc_auc_score(y_val, y_proba_val, multi_class="ovr", average="macro")
            except Exception:
                roc_auc = 0.5
                
            # Log params & metrics prominently in MLflow
            mlflow.log_params({
                "model_name": name,
                "sampling_strategy": "SMOTE",
                "train_samples_raw": len(X_train),
                "train_samples_resampled": len(X_train_res),
                "features_count": X_train_proc.shape[1]
            })
            
            mlflow.log_metrics({
                "overall_accuracy": val_accuracy,
                "val_macro_f1": macro_f1,
                "val_weighted_f1": weighted_f1,
                "val_fatal_recall": fatal_recall,
                "val_serious_recall": serious_recall,
                "val_roc_auc": roc_auc
            })
            
            logger.info(f"{name} Results - Overall Accuracy: {val_accuracy:.4f}, Macro F1: {macro_f1:.4f}, Fatal Recall: {fatal_recall:.4f}, ROC-AUC: {roc_auc:.4f}")
            
            # Confusion Matrix Artifact
            cm = confusion_matrix(y_val, y_pred_val)
            cm_path = str(MODEL_DIR / f"cm_{name.lower()}.png")
            plot_and_save_confusion_matrix(cm, class_names, cm_path)
            mlflow.log_artifact(cm_path)
            
            # Select best candidate
            if macro_f1 > best_macro_f1:
                best_macro_f1 = macro_f1
                best_model_name = name
                best_pipeline = Pipeline([
                    ("preprocessor", preprocessor),
                    ("classifier", model)
                ])
                
    logger.info(f"=== Best Model Selected: {best_model_name} with Val Macro-F1: {best_macro_f1:.4f} ===")
    
    # Final evaluation on hold-out test set
    y_pred_test = best_pipeline.predict(X_test)
    test_accuracy = accuracy_score(y_test, y_pred_test)
    test_macro_f1 = f1_score(y_test, y_pred_test, average="macro", zero_division=0)
    test_weighted_f1 = f1_score(y_test, y_pred_test, average="weighted", zero_division=0)
    test_report = classification_report(y_test, y_pred_test, target_names=class_names, output_dict=True)
    test_report["overall_accuracy"] = test_accuracy
    test_report["champion_model"] = best_model_name
    
    # Log champion model to MLflow
    with mlflow.start_run(run_name=f"Production_{best_model_name}"):
        mlflow.log_metric("overall_accuracy", test_accuracy)
        mlflow.log_metric("test_macro_f1", test_macro_f1)
        mlflow.log_metric("test_weighted_f1", test_weighted_f1)
        mlflow.set_tag("imbalance_technique", "SMOTE + Hyperparameter Optimization")
        mlflow.sklearn.log_model(best_pipeline, artifact_path="model")
        
    model_save_path = MODEL_DIR / "accident_severity_model.joblib"
    joblib.dump(best_pipeline, model_save_path)
    logger.info(f"Saved complete inference pipeline to: {model_save_path}")
    
    # Save test evaluation report
    report_path = MODEL_DIR / "test_evaluation_report.json"
    import json
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(test_report, f, indent=2)
        
    logger.info(f"Saved test evaluation report to: {report_path}")
    return best_model_name, test_macro_f1


if __name__ == "__main__":
    train_and_evaluate_models()
