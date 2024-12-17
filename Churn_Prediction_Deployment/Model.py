import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split, cross_val_score, GridSearchCV, cross_val_predict
from sklearn.metrics import confusion_matrix, classification_report, accuracy_score
from sklearn.ensemble import RandomForestClassifier, GradientBoostingClassifier, StackingClassifier
from sklearn.linear_model import LogisticRegression
from datetime import datetime
import pickle
# joblib
from datetime import datetime
import pandas as pd
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
#Self Libraries
from DB.DBConstants import *
from Functions.time_stamp import *
from Functions.helper_functions import *
logger.debug(f"Imported all libraries")
logger.debug(f"{time_period}")
logger.debug(f"Ride SQL queries")


Data_to_predict_query = """
     select 
rider_id,registered_on,first_search_date,total_searches,gap_in_first_n_last_booking,ByDriverCanc,ByUserCanc,ByAppOrMerchantCanc,total_bookings,
rides_in_first_30_days,total_rides_overall,total_dist_travelled_overall,total_fair_paid,last_ride_date
from
atlas_app.person_meta
where
(registered_on > '2024-11-18 06:55:00' or last_search_date>'2024-11-18 6:55:00')
"""
Data_to_predict = clickhouse_connection(Data_to_predict_query)

Data_to_predict['registered_on'] = pd.to_datetime(Data_to_predict['registered_on'])
Data_to_predict['first_search_date'] = pd.to_datetime(Data_to_predict['first_search_date'] )
Data_to_predict['last_ride_date'] = pd.to_datetime(Data_to_predict['last_ride_date'])
Data_to_predict.dropna(subset='last_ride_date',inplace=True)
Data_to_predict['first_search_n_reg_gap'] = (Data_to_predict['first_search_date'] - Data_to_predict['registered_on']).dt.days
Data_to_predict['average_fare'] = Data_to_predict['total_fair_paid']*1000/Data_to_predict['total_dist_travelled_overall']
Data_to_predict['booking_ride_rate'] = Data_to_predict['total_rides_overall']*100/Data_to_predict['total_bookings']
Data_to_predict['Avg_bookings_per_day'] = Data_to_predict['total_bookings']/Data_to_predict['gap_in_first_n_last_booking']

Data_to_predict.fillna(0,inplace=True)

Data_to_predict.drop(columns=['registered_on','first_search_date','last_ride_date','total_fair_paid','total_dist_travelled_overall','total_bookings','total_rides_overall','gap_in_first_n_last_booking'],inplace=True)

Data_to_predict = Data_to_predict[~Data_to_predict.isin([np.inf, -np.inf]).any(axis=1)]



Input_cols_to_predict = Data_to_predict.drop(columns=['rider_id','Churn_status'])

numeric_features = Input_cols_to_predict.select_dtypes(include=[np.number]).columns


# Load the saved model from the pickle file
with open('stacking_model.pkl', 'rb') as f:
    loaded_model = pickle.load(f)

print("Model loaded successfully from stacking_model.pkl")

new_data = Input_cols_to_predict
# Use the loaded model to make predictions
y_pred_new = loaded_model.predict(new_data)

# Create a DataFrame with the predictions and the corresponding IDs (if you have an ID column)
predictions_df = pd.DataFrame({
    'Predicted_Label': y_pred_new
})

# Optionally, you can include an ID column if needed. For example, if your new data has an 'ID' column:
predictions_df['Rider_id'] = new_data['Rider_id']

# Save the predictions to a CSV file
# predictions_df.to_csv('predictions.csv', index=False)




### Inserting Predicted values in 
insert_data_from_csv(predictions_df,'atlas_app.person_meta')

