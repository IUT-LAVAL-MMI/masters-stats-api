import logging
from functools import partial
from math import isnan
from typing import Dict, Mapping, Tuple

import pandas as pd

from masterStats.MasterStatsManager import MasterStatsManager
from masterStats.search.StatSearchOptions import StatSearchOptions
from masterStats.search.StatSearchResult import StatSearchResult

LOG = logging.getLogger(__name__)


class MongoStatSearchResult(StatSearchResult):
    def __init__(self, request_options: StatSearchOptions):
        super().__init__(request_options)

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
        for cand in self.candidatures_found:
            cand_dict = dict()
            for creator in comp_creators:
                key, component = creator(cand)
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
            comp_creators = comp_creators + [create_ins_general, create_ins_emplois, create_ins_salaire,
                                             create_ins_ref_region]
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
        for inspro in self.insertions_pro_found:
            inspro_dict = dict()
            for creator in comp_creators:
                key, component = creator(inspro)
                inspro_dict[key] = component
            yield inspro_dict


def null_if_na(v):
    return None if isnan(v) else v


def create_dict_from_document_keys(document: Mapping, *largs) -> dict:
    return dict((k, document[k]) for k in largs)


def create_cand_identifiants(cand: Mapping) -> Tuple[str, Dict]:
    return 'identifiants', create_dict_from_document_keys(cand, 'anneeCollecte', 'etabUai', 'formationIfc')


def create_cand_relations(cand: Mapping) -> Tuple[str, Dict]:
    return 'relations', create_dict_from_document_keys(cand, 'academieId', 'regionId', 'secDiscId', 'discId',
                                                       'mentionId')


def create_cand_general(cand: Mapping) -> Tuple[str, Dict]:
    return 'general', {
        'capacite': null_if_na(cand.get('col')),
        'nb': null_if_na(cand.get('n_can')),
        'nbFemmes': null_if_na(cand.get('n_can_femme')),
        'clas': null_if_na(cand.get('n_clas')),
        'clasFemmes': null_if_na(cand.get('n_clas_femme')),
        'prop': null_if_na(cand.get('n_prop')),
        'propFemmes': null_if_na(cand.get('n_prop_femme')),
        'accept': null_if_na(cand.get('n_accept')),
        'acceptFemmes': null_if_na(cand.get('n_accept_femme')),
        'nbComp': null_if_na(cand.get('n_recrut_comp')),
        'acceptDebutPP': null_if_na(cand.get('n_accept_debut_pp')),
        'rangDernier': null_if_na(cand.get('rang_dernier')),
    }


def create_cand_experience(cand: Mapping) -> Tuple[str, Dict]:
    prop_by_cat = {
        'lg3': ['n_can_lg3', 'n_can_femme_lg3', 'n_clas_lg3', 'n_clas_femme_lg3', 'n_prop_lg3', 'n_prop_femme_lg3',
                'n_accept_lg3', 'n_accept_femme_lg3'],
        'lp3': ['n_can_lp3', 'n_can_femme_lp3', 'n_clas_lp3', 'n_clas_femme_lp3', 'n_prop_lp3', 'n_prop_femme_lp3',
                'n_accept_lp3', 'n_accept_femme_lp3'],
        'master': ['n_can_master', 'n_can_femme_master', 'n_clas_master', 'n_clas_femme_master', 'n_prop_master',
                   'n_prop_femme_master', 'n_accept_master', 'n_accept_femme_master'],
        'autre': ['n_can_autre', 'n_can_femme_autre', 'n_clas_autre', 'n_clas_femme_autre', 'n_prop_autre',
                  'n_prop_femme_autre', 'n_accept_autre', 'n_accept_femme_autre'],
        'noninscrit': ['n_can_noninscri', 'n_can_femme_noninscri', 'n_clas_noninscri', 'n_clas_femme_noninscri',
                       'n_prop_noninscri', 'n_prop_femme_noninscri', 'n_accept_noninscri', 'n_accept_femme_noninscri']
    }
    cat_keys = ['nb', 'nbFemmes', 'clas', 'clasFemme', 'prop', 'propFemmes', 'accept', 'acceptFemmes']
    component = dict((cat_key, dict((k, null_if_na(cand[v])) for (k, v) in zip(cat_keys, cats))) for (cat_key, cats) in
                     prop_by_cat.items())
    return 'experience', component


def create_cand_origine(cand: Mapping) -> Tuple[str, Dict]:
    prop_by_cat = {
        'etablissement': ['n_can_etab', 'n_clas_etab', 'n_prop_etab', 'n_accept_etab'],
        'academie': ['n_can_acad', 'n_clas_acad', 'n_prop_acad', 'n_accept_acad'],
        'region': ['n_can_acad_reg', 'n_clas_acad_reg', 'n_prop_acad_reg', 'n_accept_acad_reg']
    }
    cat_keys = ['nb', 'clas', 'prop', 'accept']
    component = dict(
        (cat_key, dict((k, null_if_na(cand[v])) for (k, v) in zip(cat_keys, cats))) for (cat_key, cats) in
        prop_by_cat.items())
    return 'origine', component


def create_ins_identifiants(ins: Mapping) -> Tuple[str, Dict]:
    return 'identifiants', {
        'anneeCollecte': ins.get('anneeCollecte'),
        'etabUai': ins.get('etabUai'),
        'moisApresDip': ins.get('nbMoisApresDip'),
        'insDiscId': ins.get('ins_disc'),
    }


def create_ins_relations(ins: Mapping, sect_discs_df: pd.DataFrame,
                         mentions_df: pd.DataFrame, cache_dict: Dict = None) -> Tuple[str, Dict]:
    ins_disc = ins['ins_disc']
    if cache_dict and ins_disc in cache_dict:
        sec_disc_ids, disc_ids, mention_ids = cache_dict[ins_disc]
    else:
        selection = sect_discs_df.loc[sect_discs_df.insDiscId == ins_disc, 'disciplineId']
        sec_disc_ids = selection.index.tolist()
        disc_ids = selection.unique().tolist()
        mention_ids = mentions_df.loc[mentions_df.secDiscId.isin(sec_disc_ids), :].index.tolist()
        if cache_dict:
            cache_dict[ins_disc] = (sec_disc_ids, disc_ids, mention_ids)

    return 'relations', {
        'academieId': ins['academieId'],
        'regionId': ins['regionId'],
        'secDiscIds': sec_disc_ids,
        'discIds': disc_ids,
        'mentionIds': mention_ids
    }


def create_ins_general(ins: Mapping) -> Tuple[str, Dict]:
    return 'general', {
        'nbResponses': null_if_na(ins.get('nombre_de_reponses')),
        'tauxReponse': null_if_na(ins.get('taux_de_reponse')),
        'pbEchantillon': ins.get('pbEchantillon'),
        'pbEchantillonRaison': null_if_na(ins.get('pbEchantillonRaison')),
    }


def create_ins_emplois(ins: Mapping) -> Tuple[str, Dict]:
    return 'emplois', {
        'cadreProIntermediaire': null_if_na(ins.get('emplois_cadre_ou_professions_intermediaires')),
        'cadre': null_if_na(ins.get('emplois_cadre')),
        'stable': null_if_na(ins.get('emplois_stables')),
        'tempsPlein': null_if_na(ins.get('emplois_a_temps_plein')),
        'exterieurRegionDip': null_if_na(ins.get('emplois_exterieurs_a_la_region_de_luniversite')),
        'femmes': null_if_na(ins.get('femmes')),
        'boursier': null_if_na(ins.get('de_diplomes_boursiers')),
    }


def create_ins_salaire(ins: Mapping) -> Tuple[str, Dict]:
    return 'salaire', {
        'netMedianTempsPlein': null_if_na(ins.get('salaire_net_median_des_emplois_a_temps_plein')),
        'brutAnnuelEstime': null_if_na(ins.get('salaire_brut_annuel_estime')),
    }


def create_ins_ref_region(ins: Mapping) -> Tuple[str, Dict]:
    return 'refRegion', {
        'tauxChomageRegional': null_if_na(ins.get('emplois_cadre_ou_professions_intermediaires')),
        'netQ1Regional': null_if_na(ins.get('salaire_net_mensuel_regional_1er_quartile')),
        'netMedianRegional': null_if_na(ins.get('salaire_net_mensuel_median_regional')),
        'netQ3Regional': null_if_na(ins.get('salaire_net_mensuel_regional_3eme_quartile')),
    }
