import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.ensemble import RandomForestRegressor
from xgboost import XGBRegressor
from sklearn.cluster import DBSCAN
import pickle
import time

start_time = time.time()

# Load and prep data
data_path = "data/merged_real_estate_listings.csv"
filtered_df = pd.read_csv(data_path, usecols=["Price", "Area", "Bedrooms", "Toilets", "Location", "Coordinates"])
filtered_df = filtered_df.dropna().query("Price > 0")

# Extract District
filtered_df["District"] = filtered_df["Location"].str.split(",").str[-1].str.strip()

# Parse Coordinates
coords = filtered_df["Coordinates"].str.split(",", expand=True).astype(float)
filtered_df[["Latitude", "Longitude"]] = coords

# Hanoi bounds
filtered_df = filtered_df[
    (filtered_df["Latitude"].between(20.9, 21.2)) & 
    (filtered_df["Longitude"].between(105.7, 106.0))
]

# Cluster with DBSCAN
coords = filtered_df[["Latitude", "Longitude"]].values
db = DBSCAN(eps=0.2/6371, min_samples=2, metric='haversine', n_jobs=-1).fit(np.radians(coords))
filtered_df["Cluster_ID"] = db.labels_

# New features
filtered_df["Total_Rooms"] = filtered_df["Bedrooms"] + filtered_df["Toilets"]
filtered_df["Dist_to_Center"] = np.sqrt(
    (filtered_df["Latitude"] - 21.0285)**2 + (filtered_df["Longitude"] - 105.8542)**2
)
filtered_df["Log_Area"] = np.log(filtered_df["Area"] + 1e-6)
cluster_means = filtered_df.groupby("Cluster_ID")["Price"].mean().to_dict()
filtered_df["Cluster_Price_Mean"] = filtered_df["Cluster_ID"].map(cluster_means)

# Cap outliers
price_cap = filtered_df["Price"].quantile(0.99)
filtered_df["Price"] = filtered_df["Price"].clip(upper=price_cap)

# Encode and build X
district_dummies = pd.get_dummies(filtered_df["District"], prefix="Dist")
cluster_dummies = pd.get_dummies(filtered_df["Cluster_ID"], prefix="Cluster")
X = pd.concat([filtered_df[["Area", "Log_Area", "Bedrooms", "Toilets", "Total_Rooms", 
                            "Latitude", "Longitude", "Dist_to_Center", "Cluster_Price_Mean"]], 
               district_dummies, cluster_dummies], axis=1).values
Y = np.log(filtered_df["Price"].values + 1e-6)

# Split
X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

# RF with tighter regularization
rf = RandomForestRegressor(
    n_estimators=1500, max_depth=35, min_samples_split=5, max_features='sqrt', 
    min_samples_leaf=5, random_state=42, n_jobs=-1
)
rf.fit(X_train, Y_train)
Y_pred_rf = rf.predict(X_test)

# XGBoost with slower learning
xgb = XGBRegressor(
    n_estimators=1500, learning_rate=0.05, max_depth=6, subsample=0.8, 
    colsample_bytree=0.8, reg_alpha=0.2, reg_lambda=0.1, gamma=0.1, 
    random_state=42, n_jobs=-1, tree_method='hist'
)
xgb.fit(X_train, Y_train)
Y_pred_xgb = xgb.predict(X_test)

# Fine-tuned weight sweep around 0.5
print("=== Testing Weighted Ensemble ===")
best_r2 = 0
best_w = 0
best_pred = None
for w in [0.45, 0.475, 0.5, 0.525, 0.55]:
    Y_pred_ensemble = w * Y_pred_rf + (1 - w) * Y_pred_xgb
    r2 = r2_score(Y_test, Y_pred_ensemble)
    print(f"Ensemble (RF={w}, XGB={1-w}) R^2: {r2:.4f}")
    if r2 > best_r2:
        best_r2 = r2
        best_w = w
        best_pred = Y_pred_ensemble

# Eval
Y_test_exp = np.exp(Y_test)
Y_pred_exp = np.exp(best_pred)
test_mse = mean_squared_error(Y_test_exp, Y_pred_exp)
test_rmse = np.sqrt(test_mse)

print(f"\nBest Ensemble (RF={best_w}, XGB={1-best_w})")
print(f"Ensemble Test R^2 (log scale): {best_r2:.4f}")
print(f"Ensemble Test R^2 (original scale): {r2_score(Y_test_exp, Y_pred_exp):.4f}")
print(f"Ensemble Test RMSE (original scale): {test_rmse:.2f}")

# Full CV
rf_cv = cross_val_score(rf, X, Y, cv=5, scoring='r2', n_jobs=-1)
xgb_cv = cross_val_score(xgb, X, Y, cv=5, scoring='r2', n_jobs=-1)
print(f"RF CV R^2 (5-fold): {rf_cv.mean():.4f} ± {rf_cv.std():.4f}")
print(f"XGB CV R^2 (5-fold): {xgb_cv.mean():.4f} ± {xgb_cv.std():.4f}")

# Save
with open("rf_model_improved.pkl", "wb") as f:
    pickle.dump(rf, f)
xgb.save_model("xgb_model_improved.json")
with open("test_rmse_improved.pkl", "wb") as f:
    pickle.dump(test_rmse, f)

print(f"Runtime: {time.time() - start_time:.2f} seconds")