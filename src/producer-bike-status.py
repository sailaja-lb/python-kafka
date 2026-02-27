"""
Kafka Producer - Bike Status Topic
Simulates status updates every 15 minutes per bike for 500 bikes.
500 bikes × 4 updates/hour = ~2,000 updates/hour.

Schema:
  bike_id        string     B001–B500
  status         string     "available" | "in_maintenance" | "charging"
  battery_level  integer    0–100; nullable
  last_reported  timestamp  ISO 8601 UTC
  notes          string     Optional free text
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

TOPIC = "bike-status-topic"

# --- Simulation parameters ---
NUM_BIKES = 500  # B001–B500
STATUSES = ["available", "in_maintenance", "charging"]

# 15-min cycle compressed for dev; set to 900 for real 15-min intervals
CYCLE_SECONDS = 60  # one "15-min window" every 60s in dev

# Possible maintenance/repair notes
MAINTENANCE_NOTES = [
    "chain replaced", "flat tire", "brake adjustment", "seat repair",
    "handlebar tightened", "wheel trued", "gear cable replaced",
    "pedal replaced", "light fixed", "bell replaced",
]
CHARGING_NOTES = [
    "low battery", "scheduled charge", "battery swap",
    "solar dock", "fast charge station",
]
AVAILABLE_NOTES = [
    "routine check passed", "back from maintenance", "fully charged",
    "relocated", "cleaned",
]


def generate_bike_fleet(n=NUM_BIKES):
    """Generate initial fleet of 500 bikes (B001–B500)."""
    fleet = []
    for i in range(1, n + 1):
        # ~60% have battery (electric), ~40% null (classic/manual)
        has_battery = random.random() < 0.60
        bike = {
            "bike_id": f"B{i:03d}",
            "status": "available",
            "battery_level": random.randint(20, 100) if has_battery else None,
            "has_battery": has_battery,  # internal flag, not sent in events
        }
        fleet.append(bike)
    return fleet


def update_bike_status(bike):
    """
    Simulate a 15-minute status heartbeat for one bike.
    Returns a message matching the required schema.
    """
    # Transition probabilities based on current status
    current = bike["status"]
    if current == "available":
        bike["status"] = random.choices(
            STATUSES, weights=[85, 10, 5]
        )[0]
    elif current == "in_maintenance":
        bike["status"] = random.choices(
            STATUSES, weights=[40, 55, 5]
        )[0]
    elif current == "charging":
        bike["status"] = random.choices(
            STATUSES, weights=[50, 5, 45]
        )[0]

    # Battery level logic
    if bike["has_battery"]:
        if bike["status"] == "charging":
            # Charging: battery goes up
            bike["battery_level"] = min(100, (bike["battery_level"] or 0) + random.randint(5, 20))
        elif bike["status"] == "available":
            # In use or idle: slow drain
            bike["battery_level"] = max(0, (bike["battery_level"] or 50) - random.randint(0, 5))
        elif bike["status"] == "in_maintenance":
            # Stays the same during maintenance
            pass
    else:
        bike["battery_level"] = None

    # Generate optional notes (~25% of events)
    notes = None
    if random.random() < 0.25:
        if bike["status"] == "in_maintenance":
            notes = random.choice(MAINTENANCE_NOTES)
        elif bike["status"] == "charging":
            notes = random.choice(CHARGING_NOTES)
        elif bike["status"] == "available":
            notes = random.choice(AVAILABLE_NOTES)

    event = {
        "bike_id": bike["bike_id"],
        "status": bike["status"],
        "battery_level": bike["battery_level"],
        "last_reported": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    if notes:
        event["notes"] = notes

    return event


class BikeStatusProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            acks=PRODUCER_ACKS,
            retries=PRODUCER_RETRIES,
            batch_size=PRODUCER_BATCH_SIZE,
        )
        self.fleet = generate_bike_fleet()
        logger.info(f"BikeStatusProducer initialized with {len(self.fleet)} bikes")

    def send(self, message, key=None):
        try:
            value = json.dumps(message).encode("utf-8")
            future = self.producer.send(
                TOPIC,
                value=value,
                key=key.encode("utf-8") if key else None,
            )
            meta = future.get(timeout=10)
            logger.debug(
                f"[bike_status] bike={message['bike_id']} status={message['status']} "
                f"partition={meta.partition} offset={meta.offset}"
            )
            return True
        except KafkaError as e:
            logger.error(f"[bike_status] send failed: {e}")
            return False

    def run(self):
        """
        Every cycle (simulated 15 minutes), emit a status heartbeat for every bike.
        500 bikes × 4 cycles/hour = ~2,000 messages/hour.
        """
        logger.info(
            f"Starting bike status producer — {len(self.fleet)} bikes, "
            f"cycle every {CYCLE_SECONDS}s"
        )
        try:
            cycle = 0
            while True:
                cycle += 1
                start = time.time()
                sent = 0
                failed = 0

                for bike in self.fleet:
                    event = update_bike_status(bike)
                    if self.send(event, key=event["bike_id"]):
                        sent += 1
                    else:
                        failed += 1

                self.producer.flush()
                elapsed = time.time() - start
                logger.info(
                    f"[bike_status] cycle {cycle}: sent={sent} failed={failed} "
                    f"elapsed={elapsed:.1f}s"
                )

                # Sleep for remainder of the cycle
                remaining = max(0, CYCLE_SECONDS - elapsed)
                if remaining > 0:
                    time.sleep(remaining)
        except KeyboardInterrupt:
            logger.info("Bike status producer stopped by user")
        finally:
            self.producer.flush()
            self.producer.close()
            logger.info("Bike status producer closed")


if __name__ == "__main__":
    BikeStatusProducer().run()
