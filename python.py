import pandas as pd

df = pd.read_parquet(r"C:\Users\ogoro\Desktop\Projects\Heavy SQL, Azure, Python\yellow_tripdata_2024-11.parquet")
df.to_csv(r"C:\Users\ogoro\Desktop\Projects\Heavy SQL, Azure, Python\yellow_tripdata_2024-11.csv", index=False)