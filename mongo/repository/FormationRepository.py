from typing import List, Optional

from pydantic_mongo import AbstractRepository

from mongo.dao.MongoDAO import MongoDAO
from mongo.model.Formation import Formation


class FormationRepository(AbstractRepository[Formation]):
    class Meta:
        collection_name = MongoDAO.formation_col_name

    def find_by_textsearch(self, text_search: str):
        return self.find_by({
            '$text': {
                '$search': text_search,
                '$language': 'fr'
            }
        }, projection={'ifc': 1})

    def find_by_criteria(self, etab_uais: Optional[List[str]], sec_disc_ids: Optional[List[int]],
                         depts: Optional[List[str]], text_search: Optional[str]):
        query = dict()
        if etab_uais:
            if len(etab_uais) == 1:
                query['etabUai'] = etab_uais[0]
            else:
                query['etabUai'] = {'$in': etab_uais}
        if sec_disc_ids:
            if len(sec_disc_ids) == 1:
                query['secDiscId'] = sec_disc_ids[0]
            else:
                query['secDiscId'] = {'$in': sec_disc_ids}
        if depts:
            if len(etab_uais) == 1:
                query['dept'] = depts[0]
            else:
                query['dept'] = {'$in': depts}
        if text_search:
            query['$text'] = {
                '$search': text_search,
                '$language': 'fr'
            }
        return self.find_by(query)