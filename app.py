import streamlit as st
import numpy as np
import pandas as pd
import pickle

# Load the model and metadata
@st.cache_resource
def load_model_and_metadata():
    with open('model/xgb_model.pkl', 'rb') as f:
        model = pickle.load(f)
    with open('model/metadata.pkl', 'rb') as f:
        metadata = pickle.load(f)
    return model, metadata

# Load data
xgb_model, metadata = load_model_and_metadata()
feature_names = metadata['feature_names']
district_to_cluster = metadata['district_to_cluster']
district_price_per_area = metadata['district_price_per_area']

# App title and description
st.title('Hanoi Real Estate Price Predictor')
st.write('Enter property details to predict the market price')

# Input form
with st.form('prediction_form'):
    col1, col2 = st.columns(2)
    
    with col1:
        area = st.number_input('Area (m²)', min_value=10.0, max_value=1000.0, value=50.0)
        bedrooms = st.number_input('Bedrooms', min_value=1, max_value=10, value=2)
    
    with col2:
        toilets = st.number_input('Toilets', min_value=1, max_value=10, value=2)
        district = st.selectbox('District', list(district_to_cluster.keys()))

    submit_button = st.form_submit_button(label='Predict Price')

# Prediction function
def prepare_input(area, bedrooms, toilets, district):
    log_area = np.log(area + 1e-6)
    price_per_area = district_price_per_area.get(district, np.mean(list(district_price_per_area.values())))
    cluster_price_mean = np.mean([v for v in district_to_cluster.values() if v != -1])
    
    input_data = np.zeros(len(feature_names))
    feature_dict = {
        'Log_Area': log_area,
        'Area': area,
        'Bedrooms': bedrooms,
        'Toilets': toilets,
        'Price_per_Area': price_per_area,
        'Cluster_Price_Mean': cluster_price_mean,
    }
    
    for i, fname in enumerate(feature_names):
        if fname in feature_dict:
            input_data[i] = feature_dict[fname]
        elif fname == f'Dist_{district}':
            input_data[i] = 1
    
    return input_data.reshape(1, -1)

# Make prediction when form is submitted
if submit_button:
    input_data = prepare_input(area, bedrooms, toilets, district)
    log_pred = xgb_model.predict(input_data)[0]
    predicted_price = np.exp(log_pred)  # In VND (billions)
    
    # Display only requested output
    st.subheader('Prediction Results')
    st.write(f'Predicted Price (VND): {predicted_price:,.2f}')
    
    lower_bound = predicted_price * 0.9
    upper_bound = predicted_price * 1.1
    st.write(f'Estimated Range (VND): {lower_bound:,.2f} - {upper_bound:,.2f}')
    
    predicted_price_per_m2 = (predicted_price / area) * 1000
    st.write(f'Predicted Price per m² (VND/m²): {predicted_price_per_m2:,.2f}')