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
    alternance: Optional[bool] = None
    etabUai: Optional[str] = None
    mentionId: Optional[int] = None
    sectDiscId: Optional[int] = None
    ville: Optional[str] = None
    code_postal: Optional[int] = None
    dept: Optional[int] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None

    def to_small_dict(self) -> dict:
        return dict(ifc=self.ifc, parcours=self.parcours, alternance=self.alternance, lieux=self.lieux,
                    etabUai=self.etabUai, mentionId=self.mentionId, secDiscId=self.sectDiscId,
                    ville=self.ville, codePostal=self.code_postal, dept=self.dept, latitude=self.latitude,
                    longitude=self.longitude)

    def to_full_dict(self) -> dict:
        d = self.dict()
        d.pop('id')
        return d
