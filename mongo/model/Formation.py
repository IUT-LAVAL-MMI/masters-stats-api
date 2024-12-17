from typing import Optional
import pydantic
from pydantic import BaseModel
from pydantic_mongo import PydanticObjectId

__all__ = ['Formation']


class Formation(BaseModel):
    id: Optional[PydanticObjectId] = None
    ifc: Optional[pydantic.StrictStr]
    lieux: Optional[str] = None
    etablissement: Optional[str] = None
    academie: Optional[str] = None
    region: Optional[str] = None
    parcours: Optional[str] = None
    mention: Optional[str] = None
    discipline: Optional[str] = None
    secteur_disciplinaire: Optional[str] = None
