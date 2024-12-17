import logging
from typing import Dict, Optional, List

import pandas as pd

from masterStats.loading.candidatures_loading import load_candidates, create_academies, create_etablissements, \
    create_secteur_disciplinaires, create_mentions, create_formations, create_stats_candidatures
from masterStats.loading.cities_loading import load_cities
from masterStats.loading.disc_mapping_loading import load_disc_mapping
from masterStats.loading.insertion_pro_loading import load_insertionspro, create_stats_insertionspro
from mongo.dao.MongoDAO import MongoDAO
from mongo.model.Candidature import Candidature
from mongo.model.Formation import Formation
from mongo.model.InsertionPro import InsertionPro
from mongo.repository.CandidatureRepository import CandidatureRepository
from mongo.repository.FormationRepository import FormationRepository
from mongo.repository.InsertionProRepository import InsertionProRepository
from utils.Singleton import Singleton

__all__ = ['MasterStatsManager']

LOG = logging.getLogger(__name__)


class MasterStatsManager(metaclass=Singleton):
    __slots__ = ['__configuration', '_academies_df', '_etablissements_df', '_sect_discs_df',
                 '_mentions_df', '_formations_df', '_stats_candidatures_df', '_stats_inspros_df']

    """
    Index and Columns of datasets:
    - academies: id (idx) [int], nom [str], regionId [int], regionNom [str]
    - etablissements: uai (idx) [str], nom [str], academieId [int]
    - sect_discs: id (idx) [int], nom [str], disciplineId [int], disciplineNom [str]
    - mentions: id (idx) [int], nom [str], sectDiscId [int]
    - formations: ifc (idx), parcours [str], alternance [bool], lieux [str], etabUai [str], mentionId, ville [str], dept [str]
    """

    def __init__(self, configuration: Dict = None):
        self.__configuration: Dict = configuration
        self._academies_df: Optional[pd.DataFrame] = None
        self._etablissements_df: Optional[pd.DataFrame] = None
        self._sect_discs_df: Optional[pd.DataFrame] = None
        self._mentions_df: Optional[pd.DataFrame] = None
        self._formations_df: Optional[pd.DataFrame] = None
        self._stats_candidatures_df: Optional[pd.DataFrame] = None
        self._stats_inspros_df: Optional[pd.DataFrame] = None

    @property
    def configuration(self) -> Dict:
        return self.__configuration

    @property
    def academies_df(self) -> Optional[pd.DataFrame]:
        return self._academies_df

    @property
    def etablissements_df(self) -> Optional[pd.DataFrame]:
        return self._etablissements_df

    @property
    def sect_discs_df(self) -> Optional[pd.DataFrame]:
        return self._sect_discs_df

    @property
    def mentions_df(self) -> Optional[pd.DataFrame]:
        return self._mentions_df

    @property
    def formations_df(self) -> Optional[pd.DataFrame]:
        return self._formations_df

    @property
    def stats_candidatures_df(self) -> Optional[pd.DataFrame]:
        return self._stats_candidatures_df

    @property
    def stats_insertionspro_df(self) -> Optional[pd.DataFrame]:
        return self._stats_inspros_df

    def search_formations_ifc(self, search: str):
        mongo_dao = MongoDAO()
        formation_repo: FormationRepository = FormationRepository(mongo_dao.database)
        matching_formations_ifc = formation_repo.find_by_textsearch(search)
        return [f.ifc for f in matching_formations_ifc]

    def search_formations(self, etab_uais: Optional[List[str]], sec_disc_ids: Optional[List[str]],
                          depts: Optional[List[str]], text_search: Optional[str]):
        sec_disc_ids_int = None
        if sec_disc_ids:
            sec_disc_ids_int = [int(sd) for sd in sec_disc_ids]
        mongo_dao = MongoDAO()
        formation_repo: FormationRepository = FormationRepository(mongo_dao.database)
        return list(formation_repo.find_by_criteria(etab_uais, sec_disc_ids_int, depts, text_search))

    def build_api_stats(self):
        LOG.info("Load base CSV stats")
        self._build_api_candidates_model()

    def build_full_stats(self):
        LOG.info("Load full CSV stats")
        self._build_candidates_models()
        self._build_insertionspro_models()

    def build_mongo_cache(self, clear_col: bool = False):
        mongo_dao = MongoDAO()
        formation_repo: FormationRepository = FormationRepository(mongo_dao.database)
        candidature_repo: CandidatureRepository = CandidatureRepository(mongo_dao.database)
        insertionpro_repo: InsertionProRepository = InsertionProRepository(mongo_dao.database)

        test_presence = next(formation_repo.get_collection().find({}, limit=1, projection={'id': 1}), None)
        if test_presence and not clear_col:
            LOG.info("Mongo cache already built for formation. Do not reconstuct")
        else:
            if test_presence:
                LOG.info("Mongo cache already built for formation. Clear it before reconstructing it")
                formation_repo.get_collection().delete_many({})
            LOG.info("Build mongo cache for formation")
            for formation in self._generate_formation_mongo_doc():
                formation_repo.save(formation)

        test_presence = next(candidature_repo.get_collection().find({}, limit=1, projection={'id': 1}), None)
        if test_presence and not clear_col:
            LOG.info("Mongo cache already built for candidatures. Do not reconstuct")
        else:
            if test_presence:
                LOG.info("Mongo cache already built for candidatures. Clear it before reconstructing it")
                candidature_repo.get_collection().delete_many({})
            LOG.info("Build mongo cache for candidature")
            for candidature in self._generate_candidature_mongo_doc():
                candidature_repo.save(candidature)

        test_presence = next(insertionpro_repo.get_collection().find({}, limit=1, projection={'id': 1}), None)
        if test_presence and not clear_col:
            LOG.info("Mongo cache already built for insertions pro. Do not reconstuct")
        else:
            if test_presence:
                LOG.info("Mongo cache already built for insertions pro. Clear it before reconstructing it")
                insertionpro_repo.get_collection().delete_many({})
            LOG.info("Build mongo cache for insertions pro")
            for inspro in self._generate_insertionpro_mongo_doc():
                insertionpro_repo.save(inspro)

    def _generate_formation_mongo_doc(self):
        for row_idx, row in self._formations_df.reset_index().iterrows():
            yield Formation(
                ifc=row['ifc'],
                lieux=row['lieux'],
                parcours=row['parcours'],
                alternance=row['alternance'],
                etabUai=row['etabUai'],
                mentionId=row['mentionId'],
                sectDiscId=row['sectDiscId'],
                ville=row['ville'],
                code_postal=row['code_postal'] if not pd.isna(row['code_postal']) else None,
                dept=row['dept'] if not pd.isna(row['dept']) else None,
                latitude=row['latitude'] if not pd.isna(row['latitude']) else None,
                longitude=row['longitude'] if not pd.isna(row['longitude']) else None,

                etablissement=row['etablissement'],
                mention=row['mention'],
                secteur_disciplinaire=row['secteur_disci_lib'],
                academie=row['acad_lib'],
                region=row['acad_reg_lib'],
                discipline=row['disci_lib'],
            )

    def _generate_candidature_mongo_doc(self):
        for rowidx, row in self._stats_candidatures_df.iterrows():
            yield Candidature(**row.to_dict())

    def _generate_insertionpro_mongo_doc(self):
        for rowidx, row in self._stats_inspros_df.iterrows():
            yield InsertionPro(**row.to_dict())

    def _build_api_candidates_model(self):
        LOG.info("Load candidates and disc mapping dfs")
        base_cand_df = load_candidates(self.__configuration.get('CANDIDATURE_SOURCE'))
        base_mapping_df = load_disc_mapping(self.__configuration.get('DISC_MAPPING_SOURCE'))
        LOG.info("create academies, etablissements, sect. disc., mentions")
        self._academies_df = create_academies(base_cand_df)
        self._etablissements_df = create_etablissements(base_cand_df)
        self._sect_discs_df = create_secteur_disciplinaires(base_cand_df, base_mapping_df)
        self._mentions_df = create_mentions(base_cand_df)
        # Reste index for performance improvement on access
        self._academies_df.reset_index(inplace=True)
        self._etablissements_df.reset_index(inplace=True)
        self._sect_discs_df.reset_index(inplace=True)
        self._mentions_df.reset_index(inplace=True)

    def _build_candidates_models(self):
        LOG.info("Load candidates and disc mapping dfs")
        base_cand_df = load_candidates(self.__configuration.get('CANDIDATURE_SOURCE'))
        base_mapping_df = load_disc_mapping(self.__configuration.get('DISC_MAPPING_SOURCE'))
        cities_df = load_cities(self.__configuration.get('CITIES_SOURCE'))
        LOG.info("create academies, etablissements, sect. disc., mentions, formations and stats")
        self._academies_df = create_academies(base_cand_df)
        self._etablissements_df = create_etablissements(base_cand_df)
        self._sect_discs_df = create_secteur_disciplinaires(base_cand_df, base_mapping_df)
        self._mentions_df = create_mentions(base_cand_df)
        self._formations_df = create_formations(base_cand_df, self._mentions_df, cities_df)
        self._stats_candidatures_df = create_stats_candidatures(base_cand_df, self._formations_df)

    def _build_insertionspro_models(self):
        LOG.info("Load insertions pro dfs")
        base_inspro_df = load_insertionspro(self.__configuration.get('INSERTION_SOURCE'))
        self._stats_inspros_df = create_stats_insertionspro(base_inspro_df, self._etablissements_df,
                                                            self._academies_df)
