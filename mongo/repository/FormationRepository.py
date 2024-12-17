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
