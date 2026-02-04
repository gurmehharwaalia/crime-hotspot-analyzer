import random
import time
import statistics
from typing import List, Tuple

import psycopg2
from pymongo import MongoClient

N_POINTS = 20000
N_RUNS = 10
WARMUP_RUNS = 2

CENTER_LAT = 47.6101
CENTER_LON = -122.2015
RADIUS_METERS = 1000

POLYGON = [
    [CENTER_LON - 0.02, CENTER_LAT - 0.01],
    [CENTER_LON + 0.02, CENTER_LAT - 0.01],
    [CENTER_LON + 0.02, CENTER_LAT + 0.01],
    [CENTER_LON - 0.02, CENTER_LAT + 0.01],
    [CENTER_LON - 0.02, CENTER_LAT - 0.01],
]

POSTGRES = dict(
    host="localhost",
    port=5432,
    dbname="kagsdb",
    user="kags",
    password="kags",
)

MONGO_URI = "mongodb://localhost:27017"
MONGO_DB = "kagsdb"
MONGO_COLLECTION = "locations"


def generate_points(n: int) -> List[Tuple[float, float]]:
    pts = []
    for _ in range(n):
        lat = CENTER_LAT + random.uniform(-0.05, 0.05)
        lon = CENTER_LON + random.uniform(-0.05, 0.05)
        pts.append((lat, lon))
    return pts


def setup_postgis(points: List[Tuple[float, float]]):
    conn = psycopg2.connect(**POSTGRES)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE locations;")

    chunk = 2000
    for i in range(0, len(points), chunk):
        batch = points[i:i+chunk]
        args = ",".join(
            cur.mogrify(
                "(%s,%s, ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography)",
                (lat, lon, lon, lat)
            ).decode("utf-8")
            for lat, lon in batch
        )
        cur.execute("INSERT INTO locations (lat, lon, geom) VALUES " + args + ";")

    cur.close()
    conn.close()


def setup_mongo(points: List[Tuple[float, float]]):
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    col.drop()

    docs = []
    for idx, (lat, lon) in enumerate(points):
        docs.append({
            "id": idx,
            "location": {"type": "Point", "coordinates": [lon, lat]}
        })

    col.insert_many(docs)
    col.create_index([("location", "2dsphere")])
    client.close()


def time_runs(fn, label: str):
    times = []

    for _ in range(WARMUP_RUNS):
        fn()

    for _ in range(N_RUNS):
        t0 = time.perf_counter()
        fn()
        t1 = time.perf_counter()
        times.append((t1 - t0) * 1000.0)

    avg = statistics.mean(times)
    stdev = statistics.pstdev(times)
    print(f"{label}: avg={avg:.2f} ms, stdev={stdev:.2f} ms, runs={N_RUNS}")


def postgis_radius_query():
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM locations
        WHERE ST_DWithin(
            geom,
            ST_SetSRID(ST_MakePoint(%s,%s),4326)::geography,
            %s
        );
    """, (CENTER_LON, CENTER_LAT, RADIUS_METERS))
    cur.fetchone()
    cur.close()
    conn.close()


def postgis_polygon_query():
    wkt = "POLYGON((" + ",".join([f"{lon} {lat}" for lon, lat in POLYGON]) + "))"
    conn = psycopg2.connect(**POSTGRES)
    cur = conn.cursor()
    cur.execute("""
        SELECT COUNT(*)
        FROM locations
        WHERE ST_Contains(
          ST_GeomFromText(%s, 4326),
          (geom::geometry)
        );
    """, (wkt,))
    cur.fetchone()
    cur.close()
    conn.close()


def mongo_radius_query():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]
    list(col.find({
        "location": {
            "$nearSphere": {
                "$geometry": {"type": "Point", "coordinates": [CENTER_LON, CENTER_LAT]},
                "$maxDistance": RADIUS_METERS
            }
        }
    }, {"_id": 0}).limit(5000))
    client.close()


def mongo_polygon_query():
    client = MongoClient(MONGO_URI)
    col = client[MONGO_DB][MONGO_COLLECTION]
    list(col.find({
        "location": {
            "$geoWithin": {
                "$geometry": {"type": "Polygon", "coordinates": [POLYGON]}
            }
        }
    }, {"_id": 0}).limit(5000))
    client.close()


def main():
    print("Generating points...")
    points = generate_points(N_POINTS)

    print("Loading PostGIS...")
    setup_postgis(points)

    print("Loading MongoDB...")
    setup_mongo(points)

    print("\n--- Benchmarks (indexed, same dataset) ---")
    time_runs(postgis_radius_query, "PostGIS radius (ST_DWithin)")
    time_runs(postgis_polygon_query, "PostGIS polygon (ST_Contains)")
    time_runs(mongo_radius_query, "MongoDB radius ($nearSphere)")
    time_runs(mongo_polygon_query, "MongoDB polygon ($geoWithin)")

    print("\nDone. Screenshot these results.")


if __name__ == "__main__":
    main()


