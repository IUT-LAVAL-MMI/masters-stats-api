from pydantic_mongo import AbstractRepository

from mongo.dao.MongoDAO import MongoDAO
from mongo.model.InsertionPro import InsertionPro


class InsertionProRepository(AbstractRepository[InsertionPro]):
    class Meta:
        collection_name = MongoDAO.insertionpro_col_name