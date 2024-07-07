import streamlit as st
import pandas as pd
from pymongo import MongoClient

MONGO_URI = 'mongodb://localhost:27017/'
DB_NAME = 'depression_detection'
COLLECTION_NAME = 'social_media_data'

client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

documents = collection.find({'prediction': 1})
df = pd.DataFrame(list(documents))

# Streamlit app
st.title("Depression Risk Detection")

if not df.empty:
    st.write("Posts that may indicate depression risk:")
    st.dataframe(df)
else:
    st.write("No posts found with depression risk.")
