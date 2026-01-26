import logging
import os
import random
import re
import time
from typing import List, Tuple

from pymongo import MongoClient
from pymongo.errors import PyMongoError
from pymongo.results import UpdateResult
import simhash

from models import CanonicalThreadModel, RawEmailThreadModel
client= MongoClient(os.getenv("MONGODB_URI"))
logger= logging.getLogger(__name__)


class ThreadProcessing:
    DB_NAME= os.getenv("DB_NAME", "email")
    CT_COLL_NAME= "canonicalthread"
    RE_COLL_NAME= "rawemail"
    SIM_THRESHOLD= 0.8

    @classmethod
    def create_canonical_thread(cls, doc_id:str, raw:str, emails:List[Tuple], client:MongoClient, max_try:int=2):
        """Email Tuple is participants, subject, body"""
        if emails:
            # p is final participants
            p=emails[0][0].copy()
            if len(emails)>1:
                for email in emails[1:]:
                    for x in email[0]:
                        for i,y in enumerate(p):
                            # replace username without email with username with email if exist
                            if len(y)<len(x):
                                if y in x:
                                    p[i]=x
                                    break
                            else:
                                if x in y:
                                    break
                        else:
                            p.append(x)
            # create simhash for last email subject+body
            tmp= emails[-1][1] + emails[-1][2]
            fuzzy_key= str(simhash.Simhash(tmp).value)
            rank= len(emails)
            ctm= CanonicalThreadModel(p, fuzzy_key, rank, [doc_id])
            # mongodb cant setOnInsert and addToSet to same attribute
            # remove doc_ids from setOnInsert
            data= ctm.to_dict()
            data.pop("doc_ids")
            data.pop("participants")
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
                                filter={"fuzzy_key":fuzzy_key, "rank":rank},
                                update={
                                    "$setOnInsert":data,
                                    "$addToSet":{"doc_ids": doc_id, "participants":{"$each":p}}
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

    @staticmethod
    def normalize_body(body:str):
        if not body:
            return ""
        body= re.sub(r"\s+", " ", body).strip()
        all_ascii = re.sub(r"[^\x00-\x7F]+", r"", body)
        return all_ascii.lower()

    @staticmethod
    def split_participant_field(s:str)-> List[str]:
        if "," in s:
            l= s.split(",")
        elif ";" in s:
            l= s.split(";")
        else:
            l= [s]
        for i in range(len(l)):
            # we can store username email relationship in a new collection
            # but for this exercise we will use mongodb text search to allow
            # searching by either username or email. There is hard requirement in mongodb for searching -/ hyphen
            # must use regex search instead cause text search -x means does not include word x
            # $text search is case insensitive by default
            l[i]= re.sub(r"\s+", " ", l[i].strip())
        return l

    @staticmethod
    def normalize_subject(subject:str):
        subject= re.sub(r"^((re|fwd):\s*)+", "", subject[8:].lower())
        return re.sub(r"\s+", " ", subject).strip()

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
                    body= cls.normalize_body("\n".join(body_lines))
                    emails.append((
                        current_from,
                        current_to,
                        current_cc,
                        current_subject,
                        body
                    ))
                # Start new email
                current_from = ThreadProcessing.split_participant_field(line[5:].strip())
                current_to = None
                current_cc = None
                current_subject = None
                body_lines = []

            elif line.startswith("To:"):
                current_to = ThreadProcessing.split_participant_field(line[3:].strip())
            elif line.startswith("CC:"):
                current_cc = ThreadProcessing.split_participant_field(line[3:].strip())
            elif line.startswith("Subject:"):
                current_subject = cls.normalize_subject(line)
                # we can further normalize by lower capital, remove whitespaces
            else:
                body_lines.append(line)

        # Flush last email
        if current_from is not None:
            body= cls.normalize_body("\n".join(body_lines))
            # create unique member list, we assume from to cc set members are unique
            participants=current_from
            if isinstance(current_to, list):
                participants.extend(current_to)
            if isinstance(current_cc, list):
                participants.extend(current_cc)
            emails.append((
                participants,
                current_subject,
                body
            ))

        return emails