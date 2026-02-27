"""
Kafka Producer - Stations Topic
Simulates daily station updates for ~10% of 100 stations (capacity/status changes).
Produces ~10 updates per day cycle.

Schema:
  station_id   string     ST001–ST100
  name         string     Non-empty station name
  latitude     float      -90 → 90
  longitude    float      -180 → 180
  capacity     integer    5 → 50
  status       string     "active" | "inactive" | "maintenance"
  updated_at   timestamp  ISO 8601 UTC
"""
import json
import time
import random
import logging
from datetime import datetime, timezone
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from kafka import KafkaProducer
from kafka.errors import KafkaError
from config import (
    KAFKA_BOOTSTRAP_SERVERS,
    PRODUCER_ACKS,
    PRODUCER_RETRIES,
    PRODUCER_BATCH_SIZE
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

TOPIC = "stations-topic"

# --- Station seed data ---
NUM_STATIONS = 100  # ST001 through ST100
STATION_NAMES = [
    "Downtown Plaza", "Central Park", "Riverside Landing", "Market Square",
    "Uptown Commons", "Lakefront Dock", "University Gate", "Airport Terminal",
    "City Hall", "Tech Campus", "Hospital District", "Waterfront Pier",
    "Convention Center", "Museum Row", "Stadium North", "Transit Center",
    "Library Plaza", "Civic Center", "Harbor View", "Sunset Blvd",
    "Elm Street", "Oak Avenue", "Pine Road", "Maple Drive", "Cedar Lane",
    "Birch Way", "Willow Court", "Cherry Circle", "Aspen Trail", "Spruce Park",
    "Westside Hub", "Eastgate", "Northpoint", "Southlake", "Bayview",
    "Hilltop", "Valley Station", "Midtown", "Old Town", "Parkside",
    "Grandview", "Lakeshore", "Brookside", "Fairview", "Summit",
    "Creekside", "Ridgemont", "Meadowbrook", "Stonebridge", "Clearwater",
]
STATUSES = ["active", "inactive", "maintenance"]

# San Francisco area coordinates for realistic data
LAT_MIN, LAT_MAX = 37.7000, 37.8100
LON_MIN, LON_MAX = -122.5100, -122.3800


def generate_stations(n=NUM_STATIONS):
    """Generate a list of 100 station records (ST001–ST100)."""
    stations = []
    for i in range(1, n + 1):
        name = STATION_NAMES[i % len(STATION_NAMES)]
        station = {
            "station_id": f"ST{i:03d}",
            "name": name,
            "latitude": round(random.uniform(LAT_MIN, LAT_MAX), 4),
            "longitude": round(random.uniform(LON_MIN, LON_MAX), 4),
            "capacity": random.randint(5, 50),
            "status": "active",
        }
        stations.append(station)
    return stations


def generate_station_update(station):
    """
    Simulate a daily update for a station (capacity and/or status change).
    Returns a message matching the required schema.
    """
    # Randomly decide what changes
    change = random.choice(["capacity", "status", "both"])

    if change in ("capacity", "both"):
        station["capacity"] = random.randint(5, 50)

    if change in ("status", "both"):
        station["status"] = random.choice(STATUSES)

    return {
        "station_id": station["station_id"],
        "name": station["name"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
        "capacity": station["capacity"],
        "status": station["status"],
        "updated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


class StationsProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks=PRODUCER_ACKS,
            retries=PRODUCER_RETRIES,
            batch_size=PRODUCER_BATCH_SIZE,
        )
        self.stations = generate_stations()
        logger.info(f"StationsProducer initialized with {len(self.stations)} stations")

    def send(self, message, key=None):
        try:
            value = json.dumps(message).encode("utf-8")
            future = self.producer.send(
                TOPIC,
                value=value,
                key=key.encode("utf-8") if key else None,
            )
            meta = future.get(timeout=10)
            logger.info(
                f"[stations] {message['station_id']} status={message['status']} "
                f"capacity={message['capacity']} partition={meta.partition} offset={meta.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"[stations] send failed: {e}")
            return False

    def run(self):
        """
        Continuously simulate daily station updates.
        ~10% of stations updated per cycle. Each cycle represents one "day".
        Sleep is compressed so you can observe output quickly in dev
        (1 cycle ≈ 3600 seconds instead of 24 hours).
        """
        CYCLE_SECONDS = 3600 # one simulated "day" every 3600s for dev; set 86400 for prod
        logger.info(f"Starting stations producer — cycle every {CYCLE_SECONDS}s")
        try:
            while True:
                pct = random.uniform(0.08, 0.12)  # 8-12 %
                sample_size = max(1, int(len(self.stations) * pct))
                selected = random.sample(self.stations, sample_size)

                logger.info(f"Daily cycle: updating {len(selected)}/{len(self.stations)} stations")
                for station in selected:
                    update = generate_station_update(station)
                    self.send(update, key=update["station_id"])

                self.producer.flush()
                time.sleep(CYCLE_SECONDS)
        except KeyboardInterrupt:
            logger.info("Stations producer stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Stations producer closed")


if __name__ == "__main__":
    StationsProducer().run()
