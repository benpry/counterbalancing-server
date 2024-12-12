import os

import yaml
from bson.objectid import ObjectId
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from pymongo import MongoClient

# Connect to the MongoDB database
db_client = MongoClient(
    os.environ["MONGO_URI"]
    if "MONGO_URI" in os.environ
    else "mongodb://localhost:27017/"
)
db = db_client[os.environ["DB_NAME"] if "DB_NAME" in os.environ else "conditiondb"]
collection = db[
    os.environ["COLLECTION_NAME"] if "COLLECTION_NAME" in os.environ else "conditions"
]

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


async def get_condition(pid: str, n_conditions: int):
    """
    Find a chain that begins with the "condition" string, isn't busy, and has the smallest number of writes and reads.
    Plus make sure it has at least one write.
    """
    pipeline = [
        {"$match": {"pids": {"$nin": [pid]}}},
        {"$sort": {"n_completions": -1, "n_in_progress": -1}},
        {"$sample": {"size": 1}},
    ]

    conditions = list(collection.aggregate(pipeline))
    if len(conditions) == 0:
        return None
    else:
        return conditions[0]


@app.get("/assign/{pid}")
async def assign(pid: str):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    n_conditions = collection.count_documents({})
    condition = await get_condition(pid, n_conditions)
    if condition is None:
        return 404

    condition["n_in_progress"] += 1
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition


@app.post("/complete/{pid}/{condition}")
async def complete(pid: str, condition: int):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    condition = collection.find_one({"condition": condition})
    if condition is None:
        return 404

    condition["n_in_progress"] -= 1
    condition["n_completions"] += 1
    condition["pids"].append(pid)
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition


class SetupBody(BaseModel):
    conditions: list


@app.post("/reset")
async def reset(body: SetupBody):
    """
    Reset the database
    """
    collection.delete_many({})

    to_insert = []
    for condition in body.conditions:
        to_insert.append(
            {
                "condition": condition,
                "n_in_progress": 0,
                "n_completions": 0,
                "pids": [],
            }
        )

    res = collection.insert_many(to_insert)

    return {
        "inserted_ids": [str(oid) for oid in res.inserted_ids],
        "acknowledged": res.acknowledged,
    }


@app.post("/abandon/{condition}")
async def abandon(condition: int):
    condition = collection.find_one({"condition": condition})
    if condition is None:
        return 404

    condition["n_in_progress"] -= 1
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition
