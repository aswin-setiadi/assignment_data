import logging
import os
import random
import re
import time
from typing import List, Optional, Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.results import UpdateResult

from models import CanonicalThreadModel, RawEmailThreadModel
client= MongoClient(os.getenv("MONGODB_URI"))
logger= logging.getLogger(__name__)



class ThreadProcessing:
    SUBJECT_RE_PATTERN= r'^((Re|Fwd):\s*)+'
    DB_NAME= os.getenv("DB_NAME", "email")
    CT_COLL_NAME= "canonicalthread"
    RE_COLL_NAME= "rawemail"

    @classmethod
    def create_canonical_thread(cls, doc_id:str, raw:str, emails:List[Tuple], client:MongoClient, max_try:int=2):
        if emails:
            Subject= emails[0][3]
            rank= len(emails)
            ctm= CanonicalThreadModel(Subject, rank, [doc_id])
            # mongodb cant setOnInsert and addToSet to same attribute
            # remove doc_ids from setOnInsert
            data= ctm.to_dict()
            data.pop("doc_ids")
            retm= RawEmailThreadModel(doc_id, raw)
            ct_coll= client[cls.DB_NAME][cls.CT_COLL_NAME]
            ret_coll= client[cls.DB_NAME][cls.RE_COLL_NAME]
            for attempt in range(max_try):
                with client.start_session() as session:
                    try:
                        # transaction only available for mongodb with replicaset
                        # for this assignment we will skip for simplicity
                        # with session.start_transaction():
                        retm_res:UpdateResult= ret_coll.update_one(
                            filter={"doc_id": retm.doc_id},
                            update={
                                "$setOnInsert": retm.to_dict(remove_id=True)
                            },
                            upsert=True, session=session
                        )
                        if retm_res.matched_count>0:
                            logger.info(f"{doc_id=} already exist...")
                        else:
                            ctm_res:UpdateResult=ct_coll.update_one(
                                filter={"Subject":Subject, "rank":rank},
                                update={
                                    "$setOnInsert":data,
                                    "$addToSet":{"doc_ids": doc_id}
                                },
                                upsert=True, session=session
                            )
                        break
                    except PyMongoError as e:
                        cls._handle_transient_error(e, attempt, max_try)

        else:
            # can put in dlq or notify slack/ email if needed
            logger.info(f"Thread with doc_id:{doc_id} has no emails")

    @staticmethod
    def _handle_transient_error(e: PyMongoError, attempt:int, max_try:int):
        if(hasattr(e, "has_error_label") and
            (e.has_error_label("TransientTransactionError") or e.has_error_label("UnknownTransactionCommitResult")) and
            attempt<max_try):
            wait_time= (2**attempt) + random.random()
            time.sleep(wait_time)
            logger.warning(f"Retryable error:{e} Retrying again in {wait_time:.2f}s...")
        else:
            raise

    @classmethod
    def split_thread(cls, text:str)-> List[Tuple]:
        emails:List[Tuple]=[]
        current_from= None
        current_to= None
        current_cc= None
        current_subject= None
        body_lines=[]

        for line in text.splitlines():
            if line.startswith("From:"):
                # Flush previous email
                if current_from is not None:
                    emails.append((
                        current_from,
                        current_to,
                        current_cc,
                        current_subject,
                        "\n".join(body_lines).strip()
                    ))
                # Start new email
                current_from = line[len("From:"):].strip()
                current_to = None
                current_cc = None
                current_subject = None
                body_lines = []

            elif line.startswith("To:"):
                current_to = line[len("To:"):].strip()
            elif line.startswith("CC:"):
                current_cc = line[len("CC:"):].strip()
            elif line.startswith("Subject:"):
                current_subject = line[len("Subject:"):].strip()
                current_subject = re.sub(cls.SUBJECT_RE_PATTERN, "", current_subject, flags=re.I)
                # we can further normalize by lower capital, remove whitespaces
            else:
                body_lines.append(line)

        # Flush last email
        if current_from is not None:
            emails.append((
                current_from,
                current_to,
                current_cc,
                current_subject,
                "\n".join(body_lines).strip()
            ))

        return emails