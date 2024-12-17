import logging
from typing import Dict, Optional

from pymongo import MongoClient, database
from pymongo.collection import Collection
from pymongo.database import Database

from mongo.model import Formation
from utils.Singleton import Singleton

__all__ = ['MongoDAO']

DEFAULT_DB = 'mastersdb'

LOG = logging.getLogger(__name__)


class MongoDAO(metaclass=Singleton):
    __slots__ = ['__configuration', '__connection', '__database_name', '__db']
    formation_col_name = 'formations'

    def __init__(self, configuration: Dict = None):
        self.__configuration: Dict = configuration
        self.__connection: Optional[MongoClient] = None
        self.__database_name: Optional[str] = None
        self.__db: database = None

    @property
    def configuration(self) -> Dict:
        return self.__configuration

    @property
    def client(self) -> MongoClient:
        return self.__connection

    @property
    def database_name(self) -> str:
        return self.__database_name

    @property
    def database(self) -> Database:
        return self.__db

    @property
    def is_opened(self) -> bool:
        return self.__connection is not None

    def open(self) -> None:
        host = self.__configuration.get('host', 'localhost')
        if 'port' in self.__configuration:
            host = "%s:%d" % (host, self.__configuration.get('port'))
        extra_params = dict()
        if 'credentials' in self.__configuration:
            creds = self.__configuration['credentials']
            if 'username' in creds:
                extra_params['username'] = creds['username']
                if 'password' in creds:
                    extra_params['password'] = creds['password']
                if 'authSource' in creds:
                    extra_params['authSource'] = creds['authSource']
                if 'authMechanism' in creds:
                    extra_params['authMechanism'] = creds['authMechanism']
        self.__database_name = self.__configuration.get('database', DEFAULT_DB)
        self.__connection = MongoClient(host, **extra_params, connect=False)
        self.__db = self.__connection[self.__database_name]
        LOG.debug("Mongo connection opened to db %s", self.__database_name)

    def close(self) -> None:
        if self.__connection is not None:
            self.__connection.close()
            self.__connection = None

    def init_indexes(self) -> None:
        if not self.is_opened:
            raise Exception('Cannot init indexes without any opened connection to Mongo')
        self.__db[MongoDAO.formation_col_name].create_index([
            ('lieux', 'text'),
            ('etablissement', 'text'),
            ('academie', 'text'),
            ('region', 'text'),
            ('parcours', 'text'),
            ('mention', 'text'),
            ('discipline', 'text'),
            ('secteur_disciplinaire', 'text'),
        ], default_language="french", name="formation_txt_index", )

    @staticmethod
    def compute_dao_options_from_app(app_config: Dict):
        option_dict = dict(host=app_config.get('MONGO_HOST', 'localhost'), port=app_config.get('MONGO_PORT', 27017),
                           database=app_config.get('MONGO_DATABASE', DEFAULT_DB))
        username = app_config.get('MONGO_USERNAME')
        password = app_config.get('MONGO_PASSWORD')
        if username and password:
            cred_dict = dict(username=username, password=password)
            auth_source = app_config.get('MONGO_AUTH_SOURCE')
            if auth_source:
                cred_dict['authSource'] = auth_source
            option_dict['credentials'] = cred_dict
        return option_dict

    def __enter__(self):
        self.open()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.close()
        except Exception as e:
            LOG.warning("Exception while closing Mongo connection: " + str(e))
