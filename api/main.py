import logging
import os

from fastapi import FastAPI, HTTPException, status
from pymongo import MongoClient

logging.basicConfig(level=logging.INFO)
logger= logging.getLogger(__name__)

MONGO_URI = os.getenv("MONGODB_URI", "mongodb://mongo:27017")
DB_NAME= os.getenv("DB_NAME","email")
mongo_client = MongoClient(MONGO_URI)

app = FastAPI(title="Email Ingestion API")

@app.get("/health")
def health():
    return {
        "status": "ok",
        "service": "email-ingestion",
        "version": "1.0.0"
    }

@app.get("/canonicalthreadid/{doc_id}")
def get_canonicalthread_id(doc_id: str):
    try:
        res= mongo_client[DB_NAME]["canonicalthread"].find_one(filter={"doc_ids":doc_id}, projection={"Subject":1, "rank":1, "_id":0})
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server encountered an error")
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"canonicalthread with doc_id {doc_id} not found")
    else:
        data={"result":f"{res['Subject']}{res['rank']}"}
        return data

@app.get("/docids/")
def get_doc_ids(subject: str, rank:str):
    try:
        rank_int=int(rank)
        res= mongo_client[DB_NAME]["canonicalthread"].find_one(filter={"Subject":subject, "rank":rank_int}, projection={"doc_ids":1, "_id":0})
    except ValueError as e:
        logger.exception(e)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rank query must be a valid number, {rank=} received")
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server encountered an error")
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"canonicalthread with Subject={subject} and {rank=} not found")
    else:
        data={"result":res["doc_ids"]}
        return data

@app.get("/relations/")
def get_relations(subject: str, rank:str):
    try:
        rank_int= int(rank)
        res= list(mongo_client[DB_NAME]["canonicalthread"].find(filter={"Subject":subject, "rank":{"$ne":rank_int}}, projection={"rank":1, "_id":0}))
    except ValueError as e:
        logger.exception(e)
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"rank query must be a valid number, {rank=} received")
    except Exception as e:
        logger.exception(e)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server encountered an error")
    if res is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"canonicalthread with Subject={subject} {rank=} not found")
    else:
        result={"parents":[], "children":[]}
        for doc in res:
            if doc["rank"]<rank_int and rank_int-doc["rank"]==1:
                result["parents"].append(f"{subject}{doc['rank']}")
            elif doc["rank"]>rank_int and doc["rank"]-rank_int==1:
                result["children"].append(f"{subject}{doc['rank']}")

        data={"result":result}
        return data

@app.get("/search")
async def search_items(subject: str, rank:str):
    # FastAPI automatically decodes '%20' or '+' into a space character
    return {"query_received": f"{subject}{rank}"}

