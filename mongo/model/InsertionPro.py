from typing import Optional

from pydantic import BaseModel, Extra
from pydantic_mongo import PydanticObjectId


class InsertionPro(BaseModel, extra=Extra.allow):
    id: Optional[PydanticObjectId] = None

    def get(self, attr):
        return getattr(self, attr, None)

    def __getitem__(self, item):
        return getattr(self, item)
