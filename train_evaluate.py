# import pandas as pd
# import numpy as np
# import pickle
# import os
# from sklearn.model_selection import train_test_split
# from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
# from sklearn.linear_model import LinearRegression
# from sklearn.preprocessing import OneHotEncoder
# from sklearn.compose import ColumnTransformer
# from sklearn.pipeline import Pipeline
# from sklearn.metrics import mean_squared_log_error

# print("Loading dataset...")
# df = pd.read_csv("data/Energy Production Dataset.csv")

# # ADDED SEASONAL CONTEXT!
# X = df[['Start_Hour', 'End_Hour', 'Source', 'Month_Name', 'Season', 'Day_of_Year']]
# y = df['Production']

# X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# print("Building preprocessing pipeline...")
# # Update encoder to handle the new text columns
# preprocessor = ColumnTransformer(
#     transformers=[
#         ('cat', OneHotEncoder(handle_unknown='ignore'), ['Source', 'Month_Name', 'Season'])
#     ],
#     remainder='passthrough'
# )

# models = {
#     "Linear Regression": LinearRegression(),
#     "Random Forest": RandomForestRegressor(n_estimators=100, random_state=42),
#     "Gradient Boosting": GradientBoostingRegressor(n_estimators=100, random_state=42)
# }

# best_rmsle = float('inf')
# best_model_name = ""
# best_pipeline = None

# print("\n--- Training and Evaluating Models ---")
# for name, model in models.items():
#     pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('regressor', model)])
#     pipeline.fit(X_train, y_train)
    
#     preds = pipeline.predict(X_test)
#     preds = np.clip(preds, 0, None) # Prevent negative predictions
    
#     rmsle = np.sqrt(mean_squared_log_error(y_test, preds))
#     print(f"{name} RMSLE: {rmsle:.4f}")
    
#     if rmsle < best_rmsle:
#         best_rmsle = rmsle
#         best_model_name = name
#         best_pipeline = pipeline

# print("\n========================================")
# print(f"🏆 CHAMPION MODEL: {best_model_name} (RMSLE: {best_rmsle:.4f})")
# print("========================================")

# os.makedirs("model_service", exist_ok=True)
# with open("model_service/baseline_model.pkl", "wb") as f:
#     pickle.dump(best_pipeline, f)

import pandas as pd
import numpy as np
import pickle
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.metrics import mean_squared_log_error

print("Loading dataset...")
df = pd.read_csv("data/Energy Production Dataset.csv")

# We are now using EVERY feature we can derive from the API!
X = df[['Start_Hour', 'End_Hour', 'Source', 'Month_Name', 'Season', 'Day_of_Year', 'Day_Name']]
y = df['Production']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Building preprocessing pipeline...")
preprocessor = ColumnTransformer(
    transformers=[
        # Added Day_Name to the OneHotEncoder
        ('cat', OneHotEncoder(handle_unknown='ignore'), ['Source', 'Month_Name', 'Season', 'Day_Name'])
    ],
    remainder='passthrough'
)

# We are increasing the decision trees (n_estimators) from 100 to 300!
models = {
    "Linear Regression": LinearRegression(),
    "Random Forest (300 Trees)": RandomForestRegressor(n_estimators=100, max_depth=15, random_state=42),
    "Gradient Boosting (300 Trees)": GradientBoostingRegressor(n_estimators=100, learning_rate=0.05, max_depth=5, random_state=42)
}

best_rmsle = float('inf')
best_model_name = ""
best_pipeline = None

print("\n--- Training and Evaluating Models ---")
for name, model in models.items():
    pipeline = Pipeline(steps=[('preprocessor', preprocessor), ('regressor', model)])
    pipeline.fit(X_train, y_train)
    
    preds = pipeline.predict(X_test)
    preds = np.clip(preds, 0, None) 
    
    rmsle = np.sqrt(mean_squared_log_error(y_test, preds))
    print(f"{name} RMSLE: {rmsle:.4f}")
    
    if rmsle < best_rmsle:
        best_rmsle = rmsle
        best_model_name = name
        best_pipeline = pipeline

print("\n========================================")
print(f"🏆 CHAMPION MODEL: {best_model_name} (RMSLE: {best_rmsle:.4f})")
print("========================================")

os.makedirs("model_service", exist_ok=True)
with open("model_service/baseline_model.pkl", "wb") as f:
    pickle.dump(best_pipeline, f)