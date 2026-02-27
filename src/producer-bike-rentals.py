"""
Kafka Producer - Bike Rentals Topic
Simulates ~500–1,000 rental events per hour.

Schema:
  rental_id        string     Unique (R100001, R100002, …)
  bike_id          string     References bike_status (B001–B500)
  user_type        string     "subscriber" | "casual"
  start_time       timestamp  ISO 8601 UTC
  end_time         timestamp  ≥ start_time, ISO 8601 UTC
  station_start_id string     References stations (ST001–ST100)
  station_end_id   string     References stations (ST001–ST100)
  duration_min     integer    1 → 180
  extra            object     Optional metadata
"""
import json
import time
import random
import logging
from datetime import datetime, timezone, timedelta
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

TOPIC = "bike-rentals-topic"

# --- Simulation parameters ---
NUM_STATIONS = 100   # ST001–ST100 (matches stations topic)
NUM_BIKES = 500      # B001–B500 (matches bike_status topic)
USER_TYPES = ["subscriber", "casual"]

# Target: 500–1000 events/hour
EVENTS_PER_HOUR_MIN = 500
EVENTS_PER_HOUR_MAX = 1000

# Optional promo/extra metadata options
PROMO_CODES = ["SPRING26", "WELCOME10", "LOYALTY20", "WEEKEND15", None, None, None]


def generate_rental_event(rental_id_counter):
    """
    Generate a single bike rental event matching the required schema.
    Each event represents a completed ride with start and end times.
    """
    bike_id = f"B{random.randint(1, NUM_BIKES):03d}"
    station_start = f"ST{random.randint(1, NUM_STATIONS):03d}"
    station_end = f"ST{random.randint(1, NUM_STATIONS):03d}"

    # Duration between 1 and 180 minutes, weighted toward shorter rides
    duration_min = random.choices(
        population=[1, 3, 5, 8, 10, 15, 20, 26, 30, 45, 60, 90, 120, 150, 180],
        weights=   [2, 5, 10, 12, 15, 18, 15, 10, 8,  5,  4,  3,  2,   1,   1],
    )[0]

    start_time = datetime.now(timezone.utc)
    end_time = start_time + timedelta(minutes=duration_min)

    user_type = random.choices(USER_TYPES, weights=[65, 35])[0]

    event = {
        "rental_id": f"R{100000 + rental_id_counter}",
        "bike_id": bike_id,
        "user_type": user_type,
        "start_time": start_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_time": end_time.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "station_start_id": station_start,
        "station_end_id": station_end,
        "duration_min": duration_min,
    }

    # Optional extra metadata (~30% of rides)
    extra = {}
    promo = random.choice(PROMO_CODES)
    if promo:
        extra["promo_applied"] = True
        extra["promo_code"] = promo
    if random.random() < 0.10:
        extra["reported_issue"] = random.choice([
            "flat_tire", "brake_issue", "seat_loose", "chain_skip"
        ])
    if random.random() < 0.15:
        extra["rating"] = random.randint(1, 5)

    if extra:
        event["extra"] = extra

    return event


class BikeRentalsProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks=PRODUCER_ACKS,
            retries=PRODUCER_RETRIES,
            batch_size=PRODUCER_BATCH_SIZE,
        )
        self.rental_counter = 1
        logger.info("BikeRentalsProducer initialized")

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
                f"[bike_rentals] rental={message['rental_id']} bike={message['bike_id']} "
                f"duration={message['duration_min']}min partition={meta.partition} offset={meta.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"[bike_rentals] send failed: {e}")
            return False

    def run(self):
        """
        Continuously produce rental events at 500–1,000 per hour.
        Includes rush-hour traffic patterns.
        """
        logger.info(
            f"Starting bike rentals producer — target {EVENTS_PER_HOUR_MIN}–{EVENTS_PER_HOUR_MAX} events/hr"
        )
        try:
            while True:
                # Pick a random rate within the target range for this minute
                events_per_hour = random.randint(EVENTS_PER_HOUR_MIN, EVENTS_PER_HOUR_MAX)
                events_per_second = events_per_hour / 3600.0

                # Rush-hour pattern: higher traffic 7-9 AM and 4-7 PM UTC
                hour = datetime.now(timezone.utc).hour
                if hour in range(7, 10) or hour in range(16, 20):
                    events_per_second *= random.uniform(1.3, 1.8)
                elif hour in range(0, 6):
                    events_per_second *= random.uniform(0.1, 0.3)

                # Emit events for next 3600 seconds at the chosen rate
                batch_duration = 3600
                events_in_batch = max(1, int(events_per_second * batch_duration))
                sleep_between = batch_duration / events_in_batch

                for _ in range(events_in_batch):
                    event = generate_rental_event(self.rental_counter)
                    self.send(event, key=event["rental_id"])
                    self.rental_counter += 1
                    time.sleep(sleep_between)

                self.producer.flush()
        except KeyboardInterrupt:
            logger.info("Bike rentals producer stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Bike rentals producer closed")


if __name__ == "__main__":
    BikeRentalsProducer().run()
