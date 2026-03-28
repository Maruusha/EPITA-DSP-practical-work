import pandas as pd
import numpy as np
import pickle
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.feature_selection import SelectFromModel
from sklearn.metrics import mean_squared_log_error

def main():
    print("Starting training pipeline...")

    # --- Dynamic Path Resolution ---
    # This automatically finds the project root folder regardless of where the script is run
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.dirname(script_dir)
    
    data_path = os.path.join(project_root, "data", "Energy Production Dataset.csv")
    model_dir = os.path.join(project_root, "model_service")
    model_output_path = os.path.join(model_dir, "baseline_model.pkl")

    # 1. Load Data
    print(f"Loading dataset from {data_path}...")
    try:
        df = pd.read_csv(data_path)
    except FileNotFoundError:
        print(f"Error: Could not find dataset at {data_path}")
        return

    # Define features
    features = ['Start_Hour', 'End_Hour', 'Source', 'Month_Name', 'Season', 'Day_of_Year', 'Day_Name']
    X = df[features]
    y = df['Production']

    # 2. Split Data
    print("Splitting data...")
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # 3. Build Preprocessor
    print("Building preprocessor and feature selector...")
    categorical_cols = ['Source', 'Month_Name', 'Season', 'Day_Name']
    preprocessor = ColumnTransformer(
        transformers=[
            ('cat', OneHotEncoder(handle_unknown='ignore'), categorical_cols)
        ],
        remainder='passthrough'
    )

    # # 4. Define Feature Selector and Model
    ##version 0.3 random forest
    # feature_selector = SelectFromModel(
    #     estimator=RandomForestRegressor(n_estimators=50, random_state=42),
    #     threshold="median"
    # )

    # final_model = RandomForestRegressor(n_estimators=100, max_depth=15, random_state=42)

    # A smarter scout: 30 trees, moderate depth
    feature_selector = SelectFromModel(
        estimator=RandomForestRegressor(n_estimators=30, max_depth=10, random_state=42),
        threshold="median"
    )

    # The Goldilocks final model: 100 trees, but depth limited to 12 to prevent bloat
    final_model = RandomForestRegressor(
        n_estimators=100, 
        max_depth=12, 
        min_samples_leaf=2, 
        random_state=42
    )

    pipeline = Pipeline(steps=[
        ('preprocessor', preprocessor),
        ('feature_selection', feature_selector),
        ('regressor', final_model)
    ])

    # 5. Train and Evaluate
    print("Training Random Forest model (100 trees)...")
    pipeline.fit(X_train, y_train)
    
    preds = pipeline.predict(X_test)
    preds = np.clip(preds, 0, None)
    
    rmsle = np.sqrt(mean_squared_log_error(y_test, preds))
    print(f"Final Model RMSLE: {rmsle:.4f}")

    # 6. Save Model
    print(f"Exporting model to {model_output_path}...")
    os.makedirs(model_dir, exist_ok=True)
    with open(model_output_path, "wb") as f:
        pickle.dump(pipeline, f)

    print("Pipeline complete.")

if __name__ == "__main__":
    main()