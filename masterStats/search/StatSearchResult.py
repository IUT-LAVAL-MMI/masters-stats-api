import logging
from functools import partial
from typing import Optional, Dict
import pandas as pd
from masterStats.MasterStatsManager import MasterStatsManager
from masterStats.search.StatSearchOptions import StatSearchOptions
from masterStats.search.result_formater_utils import create_cand_identifiants, create_cand_relations, \
    create_cand_general, create_cand_experience, create_cand_origine, create_ins_general, create_ins_emplois, \
    create_ins_ref_region, create_ins_salaire, create_ins_identifiants, create_ins_relations


LOG = logging.getLogger(__name__)

class StatSearchResult:
    __slots__ = ['request_options', 'candidatures_found', 'insertions_pro_found']

    def __init__(self, request_options: StatSearchOptions):
        self.request_options: StatSearchOptions = request_options
        self.candidatures_found: Optional[pd.DataFrame] = None
        self.insertions_pro_found: Optional[pd.DataFrame] = None

    def to_dict(self) -> Dict:
        ssr_res = dict(request=self.request_options.to_dict())
        if self.candidatures_found is not None:
            ssr_res['candidatures'] = list(self._generate_cand_dicts())
        if self.insertions_pro_found is not None:
            ssr_res['insertionsPro'] = list(self._generate_inspro_dicts())
        return ssr_res

    def _generate_cand_dicts(self):
        # Build creators
        comp_creators = [create_cand_identifiants, create_cand_relations]
        if 'all' in self.request_options.cand_details:
            comp_creators = comp_creators + [create_cand_general, create_cand_experience, create_cand_origine]
        else:
            if 'general' in self.request_options.cand_details:
                comp_creators.append(create_cand_general)
            if 'experience' in self.request_options.cand_details:
                comp_creators.append(create_cand_experience)
            if 'origine' in self.request_options.cand_details:
                comp_creators.append(create_cand_origine)
        # generate cand_dict, one per row of candidatures_found
        for _, cand_row in self.candidatures_found.iterrows():
            cand_dict = dict()
            for creator in comp_creators:
                key, component = creator(cand_row)
                cand_dict[key] = component
            yield cand_dict

    def _generate_inspro_dicts(self):
        # Build creators
        sect_discs_df = MasterStatsManager().sect_discs_df
        mentions_df = MasterStatsManager().mentions_df
        relations_cache = dict()
        simp_create_ins_relations = partial(create_ins_relations, sect_discs_df=sect_discs_df,
                                            mentions_df=mentions_df, cache_dict=relations_cache)
        comp_creators = [create_ins_identifiants, simp_create_ins_relations]
        if 'all' in self.request_options.inspro_details:
            comp_creators = comp_creators + [create_ins_general, create_ins_emplois, create_ins_salaire, create_ins_ref_region]
        else:
            if 'general' in self.request_options.inspro_details:
                comp_creators.append(create_ins_general)
            if 'emplois' in self.request_options.inspro_details:
                comp_creators.append(create_ins_emplois)
            if 'salaire' in self.request_options.inspro_details:
                comp_creators.append(create_ins_salaire)
            if 'refRegion' in self.request_options.inspro_details:
                comp_creators.append(create_ins_ref_region)
        # generate inspro_dict, one per row of candidatures_found
        for _, inspro_row in self.insertions_pro_found.iterrows():
            inspro_dict = dict()
            for creator in comp_creators:
                key, component = creator(inspro_row)
                inspro_dict[key] = component
            yield inspro_dict
