from pydantic_mongo import AbstractRepository

from mongo.dao.MongoDAO import MongoDAO
from mongo.model.Candidature import Candidature


class CandidatureRepository(AbstractRepository[Candidature]):
    class Meta:
        collection_name = MongoDAO.candidature_col_name