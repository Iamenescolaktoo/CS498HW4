# Homework 4

## NetID
hcola2

## VM IP
35.192.29.220

## Files
- `app.py`
- `clean_data.py`
- `load_graph.py`
- `preprocess.py`
- `taxi_trips_clean.csv`
- `processed_data/`
- `HW4.txt`
- `Team.txt`
- `README.md`
- `fdb_answers.txt`
- `Design.pdf`

## Endpoints

### Neo4j
- `/graph-summary`
- `/top-companies?n=<int>`
- `/high-fare-trips?area_id=<int>&min_fare=<float>`
- `/co-area-drivers?driver_id=<string>`
- `/avg-fare-by-company`

### PySpark
- `/area-stats?area_id=<int>`
- `/top-pickup-areas?n=<int>`
- `/company-compare?company1=<string>&company2=<string>`
