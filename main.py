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


async def get_condition(deployment: str, pid: str):
    """
    Find a chain that begins with the "condition" string, isn't busy, and has the smallest number of writes and reads.
    Plus make sure it has at least one write.
    """
    pipeline = [
        {"$match": {"pids": {"$nin": [pid]}, "deployment": deployment}},
        {"$sort": {"n_assignments": -1}},
        {"$sample": {"size": 1}},
    ]

    conditions = list(collection.aggregate(pipeline))
    if len(conditions) == 0:
        return None
    else:
        return conditions[0]


@app.get("/assign/{deployment}/{pid}")
async def assign(deployment: str, pid: str):
    """
    Find the chain for the given condition that isn't in use and has the smallest number of completions.
    Mark that chain as in use, then return it.
    """
    condition = await get_condition(deployment, pid)
    if condition is None:
        return 404

    condition["n_assignments"] += 0.5
    condition["pids"].append(pid)
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition


@app.get("/assign/complete/{deployment}/{condition}")
async def complete(deployment: str, condition: int):
    condition = collection.find_one({"deployment": deployment, "condition": condition})
    if condition is None:
        return 404

    condition["n_assignments"] += 0.5
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition


class SetupBody(BaseModel):
    deployments: list
    conditions: list


@app.post("/reset")
async def reset(body: SetupBody):
    """
    Reset the database
    """
    collection.delete_many({})

    to_insert = []
    for deployment in body.deployments:
        for condition in body.conditions:
            to_insert.append(
                {
                    "deployment": deployment,
                    "condition": condition,
                    "n_assignments": 0,
                    "pids": [],
                }
            )

    res = collection.insert_many(to_insert)

    return {
        "inserted_ids": [str(oid) for oid in res.inserted_ids],
        "acknowledged": res.acknowledged,
    }


@app.post("/abandon/{deployment}/{condition}")
async def abandon(deployment: str, condition: int):
    condition = collection.find_one({"deployment": deployment, "condition": condition})
    if condition is None:
        return 404

    condition["n_assignments"] -= 0.5
    collection.update_one({"_id": condition["_id"]}, {"$set": condition})

    condition["_id"] = str(condition["_id"])
    return condition
