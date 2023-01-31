import os
import asyncio
from typing import cast

from devtools import debug
from client import AsyncMongoClient

DATABASE_URL = cast(str, os.getenv("MONGODB_URL"))


async def main():
    client = AsyncMongoClient(DATABASE_URL)
    db = client.get_database("database")
    example = db.get_collection("example")
    # result = await example.aggregate([{"$project": {"user": "$user", "_id": 0}}])
    result = await example.insert_one(
        {
            "user": "Jeet",
            "password": "password",
            "nested": [
                {
                    "attrs": ["one", "two", "three"],
                },
                {
                    "attrs": ["four", "five", "six"],
                },
            ],
        }
    )
    return result


if __name__ == "__main__":
    result = asyncio.run(main())
    debug(result)
