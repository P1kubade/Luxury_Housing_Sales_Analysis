import pandas as pd
from sqlalchemy import create_engine, types


df = pd.read_csv('C:/Users/kubad/Desktop/PROJECTS/Guvi_Project-2/Cleaned_Luxury_Housing.csv')



sql_schema = {
    'Property_ID': types.VARCHAR(50),
    'Micro_Market': types.VARCHAR(100),
    'Project_Name': types.VARCHAR(100),
    'Developer_Name': types.VARCHAR(100),
    'Unit_Size_Sqft': types.DECIMAL(10, 2),
    'Configuration': types.VARCHAR(20),
    'Ticket_Price_Cr': types.DECIMAL(10, 4),
    'Transaction_Type': types.VARCHAR(50),
    'Buyer_Type': types.VARCHAR(50),
    'Purchase_Quarter': types.Date(),  # Forces strict Date format
    'Connectivity_Score': types.DECIMAL(5, 2),
    'Amenity_Score': types.DECIMAL(5, 2),
    'Possession_Status': types.VARCHAR(50),
    'Sales_Channel': types.VARCHAR(50),
    'NRI_Buyer': types.Integer(),      # 1 or 0
    'Locality_Infra_Score': types.DECIMAL(5, 2),
    'Avg_Traffic_Time_Min': types.Integer(),
    'Buyer_Comments': types.Text(),    # Handles long text
    'Price_Per_Sqft': types.DECIMAL(15, 2)
}



db_host = "localhost"
db_user = "root"
db_password = "#mySql123" 
db_name = "luxury_housing"
engine = create_engine(f"mysql+pymysql://{db_user}:{db_password}@{db_host}/{db_name}")

# --- THE UPLOAD ---
print("Starting Data Injection...")

try:
    df.to_sql(
        'luxury_housing_data', 
        con=engine, 
        if_exists='replace', # Be careful: this Wipes the table if it exists
        index=False, 
        dtype=sql_schema   # Applies our strict schema
    )
    print("SUCCESS: Data loaded into table 'luxury_housing_data'.")
    
except Exception as e:
    print(f"FAILED: {e}")