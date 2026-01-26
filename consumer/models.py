from dataclasses import asdict, dataclass, field, fields, is_dataclass
import logging
from typing import List, Optional

from bson import ObjectId

logger= logging.getLogger(__name__)

class DataclassProcessingError(Exception):
    def __init__(self, message="Fail to transform data to dictionary.") -> None:
        self.message= message
        super().__init__(self.message)

def dataclass_from_dict(cls, data: dict):
    """(Credit to chatgpt) Return dataclass obj (supports nested dataclass).

    :param cls: class of dataclass type
    :type cls: dataclass
    :param data: dict to built the dataclass
    :type data: dict
    :return: dataclass obj
    :rtype: dataclass
    """
    kwargs = {}
    for f in fields(cls):
        value = data.get(f.name)
        # If the field is itself a dataclass
        if is_dataclass(f.type) and isinstance(value, dict):
            kwargs[f.name] = dataclass_from_dict(f.type, value)
        else:
            kwargs[f.name] = value
    return cls(**kwargs)

@dataclass
class Email:
    From:str
    To: str
    CC: str
    Subject: str
    body: str


@dataclass
class BaseModel:
    @classmethod
    def from_dict(cls, data: dict):
        """Convert dict to dataclass.

        :raises DataclassProcessingError: when fail to instantiate dataclass obj.
        """
        try:
            obj=dataclass_from_dict(cls, data)
        except DataclassProcessingError as e:
            logger.exception(e)
            raise
        except Exception as e:
            logger.exception(e)
            raise DataclassProcessingError("Fail to transform data to dataclass.")
        return obj

    def to_dict(self, remove_id:bool=False) -> dict:
        """Convert dataclass to dict (including nested dataclass). If the dict
        has _id attribute with value None, ObjectId(None) will generate a valid ObjectId
        using current time.

        :param remove_id: wether to remove _id when converting to dict, useful when inserting to db
        where we want mongodb engine to generate the _id instead of python process.
        :type remove_id: bool
        :raises DataclassProcessingError: when fail to instantiate dict obj.
        """
        try:
            data = asdict(self)
            if hasattr(self,"_id"):
                if remove_id:
                    data.pop("_id")
                else:
                    data["_id"] = ObjectId(getattr(self,"_id"))
        except Exception as e:
            logger.exception(e)
            raise DataclassProcessingError()
        return data

@dataclass
class CanonicalThreadModel(BaseModel):
    # the permutation of From To CC may differ between emails in the same thread
    participants:List
    fuzzy_key: str
    rank: int
    doc_ids: List[str]
    _id: Optional[ObjectId]= field(default=None)

@dataclass
class RawEmailThreadModel(BaseModel):
    doc_id: str
    raw: str
    _id: Optional[ObjectId]= field(default=None)