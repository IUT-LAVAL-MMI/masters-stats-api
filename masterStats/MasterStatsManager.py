import logging
from typing import Dict, Optional
import pandas as pd
from masterStats.loading.candidatures_loading import load_candidates, create_academies, create_etablissements, \
    create_secteur_disciplinaires, create_mentions, create_formations, create_stats_candidatures
from masterStats.loading.disc_mapping_loading import load_disc_mapping
from masterStats.loading.insertion_pro_loading import load_insertionspro, create_stats_insertionspro
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

    def build_stats(self):
        LOG.info("Load base CSV")
        self._build_candidates_models()
        self._build_insertionspro_models()

    def _build_candidates_models(self):
        LOG.info("Load candidates and disc mapping dfs")
        base_cand_df = load_candidates(self.__configuration.get('CANDIDATURE_SOURCE'))
        base_mapping_df = load_disc_mapping(self.__configuration.get('DISC_MAPPING_SOURCE'))
        LOG.info("create academies, etablissements, sect. disc., mentions, formations and stats")
        self._academies_df = create_academies(base_cand_df)
        self._etablissements_df = create_etablissements(base_cand_df)
        self._sect_discs_df = create_secteur_disciplinaires(base_cand_df, base_mapping_df)
        self._mentions_df = create_mentions(base_cand_df)
        self._formations_df = create_formations(base_cand_df, self._mentions_df)
        self._stats_candidatures_df = create_stats_candidatures(base_cand_df, self._formations_df)

    def _build_insertionspro_models(self):
        LOG.info("Load insertions pro dfs")
        base_inspro_df = load_insertionspro(self.__configuration.get('INSERTION_SOURCE'))
        self._stats_inspros_df = create_stats_insertionspro(base_inspro_df, self._etablissements_df,
                                                            self._academies_df)