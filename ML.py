import time
import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import r2_score, mean_squared_error
from xgboost import XGBRegressor
from sklearn.cluster import DBSCAN
import pickle

def house_prediction():
    start_time = time.time()

    # Load and prep data
    data_path = "data/relevance/merged_real_estate_listings_parsed.csv"
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

    # Cluster with DBSCAN (200m radius)
    coords = filtered_df[["Latitude", "Longitude"]].values
    db = DBSCAN(eps=0.000008, min_samples=2, metric='haversine', n_jobs=-1).fit(np.radians(coords))
    filtered_df["Cluster_ID"] = db.labels_

    # New features
    filtered_df["Log_Area"] = np.log(filtered_df["Area"] + 1e-6)
    cluster_means = filtered_df.groupby("Cluster_ID")["Price"].mean().to_dict()
    filtered_df["Cluster_Price_Mean"] = filtered_df["Cluster_ID"].map(cluster_means)

    # Cap outliers
    price_cap = filtered_df["Price"].quantile(0.99)
    filtered_df["Price"] = filtered_df["Price"].clip(upper=price_cap)
    filtered_df["Price_per_Area"] = filtered_df["Price"] / filtered_df["Area"] * 1000

    # Save metadata for app.py
    district_to_cluster = filtered_df.groupby("District")["Cluster_ID"].agg(lambda x: x.mode()[0] if len(x.mode()) > 0 else -1).to_dict()
    district_price_per_area = filtered_df.groupby("District")["Price_per_Area"].mean().to_dict()

    # Encode and build X
    district_dummies = pd.get_dummies(filtered_df["District"], prefix="Dist")
    X_df = pd.concat([filtered_df[["Log_Area", "Area", "Bedrooms", "Toilets", "Price_per_Area", "Cluster_Price_Mean"]], 
                    district_dummies], axis=1)
    feature_names = X_df.columns.tolist()
    X = X_df.values
    Y = np.log(filtered_df["Price"].values + 1e-6)

    # Split
    X_train, X_test, Y_train, Y_test = train_test_split(X, Y, test_size=0.2, random_state=42)

    # XGB
    xgb = XGBRegressor(
        n_estimators=600, 
        learning_rate=0.06, 
        max_depth=5, 
        subsample=0.85, 
        colsample_bytree=0.75, 
        reg_alpha=0.1, 
        reg_lambda=0.2, 
        random_state=42, 
        n_jobs=-1, 
        tree_method='hist'
    )
    xgb.fit(X_train, Y_train)
    Y_pred_xgb = xgb.predict(X_test)

    # Eval
    r2_log = r2_score(Y_test, Y_pred_xgb)
    Y_test_exp = np.exp(Y_test)
    Y_pred_exp = np.exp(Y_pred_xgb)
    test_mse = mean_squared_error(Y_test_exp, Y_pred_exp)
    test_rmse = np.sqrt(test_mse)

    # Full CV
    xgb_cv = cross_val_score(xgb, X, Y, cv=5, scoring='r2', n_jobs=-1)

    # Save the model and metadata
    with open('model/xgb_model.pkl', 'wb') as f:
        pickle.dump(xgb, f)

    metadata = {
        'feature_names': feature_names,
        'district_to_cluster': district_to_cluster,
        'district_price_per_area': district_price_per_area
    }
    with open('model/metadata.pkl', 'wb') as f:
        pickle.dump(metadata, f)