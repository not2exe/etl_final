import random
import uuid
from datetime import datetime, timedelta

from faker import Faker

import sys
import os
sys.path.insert(0, os.path.dirname(__file__))
from helpers import get_mongo_db

fake = Faker()
Faker.seed(42)
random.seed(42)

USER_IDS = [f"user_{i:04d}" for i in range(1, 101)]
PRODUCT_IDS = [f"prod_{i:04d}" for i in range(1, 51)]
EVENT_TYPES = ["click", "scroll", "purchase", "login", "logout", "search", "view", "share"]
ISSUE_TYPES = ["billing", "technical", "account", "delivery", "refund", "other"]
TICKET_STATUSES = ["open", "in_progress", "resolved", "closed"]
MODERATION_STATUSES = ["pending", "approved", "rejected", "escalated"]
DEVICES = ["mobile", "desktop", "tablet"]
BROWSERS = ["Chrome", "Firefox", "Safari", "Edge"]
PAGES = ["/home", "/products", "/cart", "/checkout", "/profile", "/search", "/about", "/faq", "/blog", "/contact"]
FLAG_TYPES = ["spam", "offensive", "misleading", "duplicate", "irrelevant"]


def random_dt(days_back=90):
    return fake.date_time_between(start_date=f"-{days_back}d", end_date="now")


def generate_user_sessions(db, count=500):
    db.user_sessions.drop()
    docs = []
    for _ in range(count):
        start = random_dt()
        duration_min = random.randint(1, 120)
        end = start + timedelta(minutes=duration_min)
        visited = random.sample(PAGES, k=random.randint(1, len(PAGES)))
        actions = [
            {"action": random.choice(EVENT_TYPES), "timestamp": (start + timedelta(minutes=random.randint(0, duration_min))).isoformat()}
            for _ in range(random.randint(1, 10))
        ]
        docs.append({
            "session_id": str(uuid.uuid4()),
            "user_id": random.choice(USER_IDS),
            "start_time": start,
            "end_time": end,
            "pages_visited": visited,
            "device": {
                "type": random.choice(DEVICES),
                "browser": random.choice(BROWSERS),
                "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
            },
            "actions": actions,
        })
    db.user_sessions.insert_many(docs)
    print(f"Inserted {len(docs)} user_sessions")


def generate_event_logs(db, count=1000):
    db.event_logs.drop()
    docs = []
    for _ in range(count):
        evt = random.choice(EVENT_TYPES)
        details = {"page": random.choice(PAGES)}
        if evt == "purchase":
            details["product_id"] = random.choice(PRODUCT_IDS)
            details["amount"] = round(random.uniform(5, 500), 2)
        elif evt == "search":
            details["query"] = fake.sentence(nb_words=3)
        docs.append({
            "event_id": str(uuid.uuid4()),
            "user_id": random.choice(USER_IDS),
            "timestamp": random_dt(),
            "event_type": evt,
            "details": details,
        })
    db.event_logs.insert_many(docs)
    print(f"Inserted {len(docs)} event_logs")


def generate_support_tickets(db, count=200):
    db.support_tickets.drop()
    docs = []
    for _ in range(count):
        created = random_dt()
        status = random.choice(TICKET_STATUSES)
        updated = created + timedelta(hours=random.randint(1, 72))
        msgs = []
        for i in range(random.randint(1, 6)):
            sender = "user" if i % 2 == 0 else "agent"
            msgs.append({
                "sender": sender,
                "message": fake.sentence(nb_words=random.randint(5, 20)),
                "timestamp": (created + timedelta(hours=i)).isoformat(),
            })
        docs.append({
            "ticket_id": str(uuid.uuid4()),
            "user_id": random.choice(USER_IDS),
            "created_at": created,
            "updated_at": updated,
            "status": status,
            "issue_type": random.choice(ISSUE_TYPES),
            "messages": msgs,
        })
    db.support_tickets.insert_many(docs)
    print(f"Inserted {len(docs)} support_tickets")


def generate_user_recommendations(db, count=100):
    db.user_recommendations.drop()
    docs = []
    used_users = random.sample(USER_IDS, min(count, len(USER_IDS)))
    for uid in used_users:
        docs.append({
            "user_id": uid,
            "recommended_products": random.sample(PRODUCT_IDS, k=random.randint(3, 10)),
            "last_updated": random_dt(days_back=30),
        })
    db.user_recommendations.insert_many(docs)
    print(f"Inserted {len(docs)} user_recommendations")


def generate_moderation_queue(db, count=300):
    db.moderation_queue.drop()
    docs = []
    for _ in range(count):
        flags = random.sample(FLAG_TYPES, k=random.randint(0, 3))
        docs.append({
            "review_id": str(uuid.uuid4()),
            "user_id": random.choice(USER_IDS),
            "product_id": random.choice(PRODUCT_IDS),
            "review_text": fake.paragraph(nb_sentences=random.randint(1, 5)),
            "rating": random.randint(1, 5),
            "moderation_status": random.choice(MODERATION_STATUSES),
            "flags": flags,
            "created_at": random_dt(),
        })
    db.moderation_queue.insert_many(docs)
    print(f"Inserted {len(docs)} moderation_queue")


def main():
    db = get_mongo_db()
    generate_user_sessions(db)
    generate_event_logs(db)
    generate_support_tickets(db)
    generate_user_recommendations(db)
    generate_moderation_queue(db)
    print("Data generation complete.")


if __name__ == "__main__":
    main()
