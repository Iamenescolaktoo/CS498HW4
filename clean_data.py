import pandas as pd

use_cols = [
    "Trip ID",
    "Taxi ID",
    "Company",
    "Pickup Community Area",
    "Dropoff Community Area",
    "Fare",
    "Trip Seconds",
]

df = pd.read_csv("taxi_trips.csv", usecols=use_cols)

df = df.rename(columns={
    "Trip ID": "trip_id",
    "Taxi ID": "driver_id",
    "Company": "company",
    "Pickup Community Area": "pickup_area",
    "Dropoff Community Area": "dropoff_area",
    "Fare": "fare",
    "Trip Seconds": "trip_seconds",
})

df = df.dropna(subset=[
    "trip_id",
    "driver_id",
    "company",
    "pickup_area",
    "dropoff_area",
    "fare",
    "trip_seconds",
]).copy()

df["pickup_area"] = pd.to_numeric(df["pickup_area"], errors="coerce")
df["dropoff_area"] = pd.to_numeric(df["dropoff_area"], errors="coerce")
df["fare"] = pd.to_numeric(df["fare"], errors="coerce")
df["trip_seconds"] = pd.to_numeric(df["trip_seconds"], errors="coerce")

df = df.dropna(subset=[
    "pickup_area",
    "dropoff_area",
    "fare",
    "trip_seconds",
]).copy()

df["pickup_area"] = df["pickup_area"].astype(int)
df["dropoff_area"] = df["dropoff_area"].astype(int)
df["trip_seconds"] = df["trip_seconds"].astype(int)

df = df[(df["fare"] > 0) & (df["trip_seconds"] > 0)].copy()

df = df.head(10000)

df.to_csv("taxi_trips_clean.csv", index=False)

print("rows:", len(df))
print("unique drivers:", df["driver_id"].nunique())
print("unique companies:", df["company"].nunique())
print("unique pickup areas:", df["pickup_area"].nunique())
print("unique dropoff areas:", df["dropoff_area"].nunique())
print("unique all areas:", pd.concat([df["pickup_area"], df["dropoff_area"]]).nunique())

