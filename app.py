from flask import Flask, jsonify,request
from neo4j import GraphDatabase
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, round as spark_round

spark = SparkSession.builder.appName("HW4API").getOrCreate()
spark_df = spark.read.csv("taxi_trips_clean.csv", header=True, inferSchema=True)
spark_df.createOrReplaceTempView("trips")

app = Flask(__name__)

URI = "bolt://localhost:7687"
USER = "neo4j"
PASSWORD = "Hw4Neo4jPass123"

driver = GraphDatabase.driver(URI, auth=(USER, PASSWORD))

@app.route("/company-compare", methods=["GET"])
def company_compare():
    company1 = request.args.get("company1")
    company2 = request.args.get("company2")

    results = spark.sql(f"""
        SELECT
            company,
            ROUND(AVG(fare), 2) AS avg_fare,
            ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds,
            COUNT(*) AS trip_count
        FROM trips
        WHERE company IN ('{company1}', '{company2}')
        GROUP BY company
    """).collect()

    data = {row["company"]: {
        "avg_fare": row["avg_fare"],
        "avg_trip_seconds": row["avg_trip_seconds"],
        "trip_count": row["trip_count"]
    } for row in results}

    if company1 not in data or company2 not in data:
        return jsonify({"error": "One or both companies not found"}), 404

    return jsonify({
        company1: data[company1],
        company2: data[company2]
    })

@app.route("/top-pickup-areas", methods=["GET"])
def top_pickup_areas():
    n = int(request.args.get("n", 5))

    results = spark.sql(f"""
        SELECT
            pickup_area AS area_id,
            COUNT(*) AS trip_count
        FROM trips
        GROUP BY pickup_area
        ORDER BY trip_count DESC, area_id ASC
        LIMIT {n}
    """).collect()

    data = [
        {
            "area_id": row["area_id"],
            "trip_count": row["trip_count"]
        }
        for row in results
    ]

    return jsonify(data)

@app.route("/graph-summary", methods=["GET"])
def graph_summary():
    query = """
    MATCH (d:Driver)
    WITH count(d) AS driver_count
    MATCH (c:Company)
    WITH driver_count, count(c) AS company_count
    MATCH (a:Area)
    WITH driver_count, company_count, count(a) AS area_count
    MATCH ()-[t:TRIP]->()
    RETURN driver_count, company_count, area_count, count(t) AS trip_count
    """
    with driver.session() as session:
        record = session.run(query).single()
        return jsonify({
            "driver_count": record["driver_count"],
            "company_count": record["company_count"],
            "area_count": record["area_count"],
            "trip_count": record["trip_count"]
        })

@app.route("/top-companies", methods=["GET"])
def top_companies():
    n = int(request.args.get("n", 5))
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    RETURN c.name AS company, count(DISTINCT d) AS driver_count
    ORDER BY driver_count DESC, company ASC
    LIMIT $n
    """
    with driver.session() as session:
        results = session.run(query, n=n)
        data = [
            {
                "company": record["company"],
                "driver_count": record["driver_count"]
            }
            for record in results
        ]
        return jsonify(data)

@app.route("/high-fare-trips", methods=["GET"])
def high_fare_trips():
    area_id = int(request.args.get("area_id"))
    min_fare = float(request.args.get("min_fare"))

    query = """
    MATCH (d:Driver)-[t:TRIP]->(a:Area)
    WHERE a.area_id = $area_id AND t.fare > $min_fare
    RETURN d.driver_id AS driver_id,
           t.trip_id AS trip_id,
           t.fare AS fare
    ORDER BY fare DESC, trip_id ASC
    """
    with driver.session() as session:
        results = session.run(query, area_id=area_id, min_fare=min_fare)
        data = [
            {
                "driver_id": record["driver_id"],
                "trip_id": record["trip_id"],
                "fare": record["fare"]
            }
            for record in results
        ]
        return jsonify(data)

@app.route("/co-area-drivers", methods=["GET"])
def co_area_drivers():
    driver_id = request.args.get("driver_id")

    query = """
    MATCH (:Driver {driver_id: $driver_id})-[:TRIP]->(a:Area)<-[:TRIP]-(other:Driver)
    WHERE other.driver_id <> $driver_id
    RETURN DISTINCT other.driver_id AS driver_id
    ORDER BY driver_id ASC
    """
    with driver.session() as session:
        results = session.run(query, driver_id=driver_id)
        data = [{"driver_id": record["driver_id"]} for record in results]
        return jsonify(data)

@app.route("/avg-fare-by-company", methods=["GET"])
def avg_fare_by_company():
    query = """
    MATCH (d:Driver)-[:WORKS_FOR]->(c:Company)
    MATCH (d)-[t:TRIP]->(:Area)
    RETURN c.name AS company,
           round(avg(t.fare), 2) AS avg_fare
    ORDER BY avg_fare DESC, company ASC
    """
    with driver.session() as session:
        results = session.run(query)
        data = [
            {
                "company": record["company"],
                "avg_fare": record["avg_fare"]
            }
            for record in results
        ]
        return jsonify(data)

@app.route("/area-stats", methods=["GET"])
def area_stats():
    area_id = int(request.args.get("area_id"))

    result = spark.sql(f"""
        SELECT
            COUNT(*) AS trip_count,
            ROUND(AVG(fare), 2) AS avg_fare,
            ROUND(AVG(trip_seconds), 2) AS avg_trip_seconds
        FROM trips
        WHERE pickup_area = {area_id}
    """).collect()[0]

    return jsonify({
        "area_id": area_id,
        "trip_count": result["trip_count"],
        "avg_fare": result["avg_fare"],
        "avg_trip_seconds": result["avg_trip_seconds"]
    })

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
