import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
load_dotenv()

og_data = os.getenv("FULL_PATH")
sample_data = os.getenv("NULL_DATA")

np.random.seed(42)
n = 100

data = {
    "engine_cc": np.random.randint(800, 3500, n),
    "vehicle_weight_kg": np.random.randint(700, 2500, n),
    "cylinders": np.random.choice([3, 4, 6], n, p=[0.2, 0.6, 0.2]),
    "fuel_type": np.random.choice(["Petrol", "Diesel", "CNG"], n, p=[0.5, 0.4, 0.1]),
    "tire_pressure_psi": np.random.randint(28, 38, n),
}

# Base mileage formula with noise
mileage = (
    50
    - (data["engine_cc"] / 200) 
    - (data["vehicle_weight_kg"] / 300)
    - (data["cylinders"] * 0.8)
    + ((np.array(data["tire_pressure_psi"]) - 32) * 0.3)
    + np.random.normal(0, 1.5, n)
)

data["mileage_kmpl"] = np.round(mileage, 2)

df = pd.DataFrame(data)
df.to_csv(og_data, index=False)
print(df)

# Make 20% of mileage null
np.random.seed(42)
null_indices = np.random.choice(df.index, size=int(0.2 * len(df)), replace=False)
df.loc[null_indices, "mileage_kmpl"] = None

# Save new file
df.to_csv(sample_data, index=False)
print(df[df["mileage_kmpl"].isna()].head(10))
