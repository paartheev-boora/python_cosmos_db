from fastapi import FastAPI, HTTPException
from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv
from bson import ObjectId
import pandas as pd
import os
import time

load_dotenv()

CONN_STRING = os.getenv("COSMOS_CONN_STRING")
DB_NAME = os.getenv("DB_NAME")
COLL_NAME = os.getenv("COLLECTION")
DATA_FILE = os.getenv("NULL_DATA")     # load null dataset
FULL_DATA_FILE = os.getenv("FULL_PATH")  # load original dataset

if not all([CONN_STRING, DB_NAME, COLL_NAME]):
    raise Exception("Missing environment variables for CosmosDB")

app = FastAPI()

client = AsyncIOMotorClient(CONN_STRING, tls=True, tlsAllowInvalidCertificates=True)
db = client[DB_NAME]
collection = db[COLL_NAME]


def fix_id(doc):
    """Convert Mongo _id to id string without mutating original object."""
    return {**doc, "id": str(doc["_id"]), "_id": None}


@app.get("/")
async def root():
    return {"message": "Cosmos App running successfully"}


# INSERT
@app.post("/cars/insert-null-data")
async def insert_null_dataset():
    if not os.path.exists(DATA_FILE):
        raise HTTPException(400, f"File not found: {DATA_FILE}")

    df = pd.read_csv(DATA_FILE)
    records = df.to_dict(orient="records")

    inserted_count = 0

    for i in range(0, len(records), 30):
        batch = records[i:i+30]

        # Remove _id (Cosmos rejects it)
        for doc in batch:
            doc.pop("_id", None)

        result = await collection.insert_many(batch)
        inserted_count += len(result.inserted_ids)
        time.sleep(0.2)

    return {"inserted_count": inserted_count, "status": "success"}


@app.post("/cars/insert-full-data")
async def insert_full_dataset():
    if not os.path.exists(FULL_DATA_FILE):
        raise HTTPException(400, f"File not found: {FULL_DATA_FILE}")

    df = pd.read_csv(FULL_DATA_FILE)
    records = df.to_dict(orient="records")

    inserted_count = 0

    for i in range(0, len(records), 30):
        batch = records[i:i+30]
        for doc in batch:
            doc.pop("_id", None)

        result = await collection.insert_many(batch)
        inserted_count += len(result.inserted_ids)
        time.sleep(0.2)

    return {"inserted_count": inserted_count, "status": "success"}


# READ ALL
@app.get("/cars")
async def read_all():
    cursor = await collection.find().to_list(None)
    return [fix_id(doc) for doc in cursor]


# READ ONE
@app.get("/cars/{id}")
async def get_car(id: str):
    try:
        doc = await collection.find_one({"_id": ObjectId(id)})
    except:
        raise HTTPException(400, "Invalid ObjectId format")

    if not doc:
        raise HTTPException(404, "Car not found")

    return fix_id(doc)
