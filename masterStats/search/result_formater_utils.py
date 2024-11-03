from typing import Dict, Tuple

import numpy as np
import pandas as pd

def null_if_na(v):
    return None if pd.isna(v) else v

def create_cand_identifiants(cand_row: pd.Series) -> Tuple[str, Dict]:
    return 'identifiants', {
        'anneeCollecte': cand_row.anneeCollecte,
        'etabUai': cand_row.etabUai,
        'formationIfc': cand_row.formationIfc
    }


def create_cand_relations(cand_row: pd.Series) -> Tuple[str, Dict]:
    return 'relations', {
        'academieId': cand_row.academieId,
        'regionId': cand_row.regionId,
        'secDiscId': cand_row.secDiscId,
        'discId': cand_row.discId,
        'mentionId': cand_row.mentionId,
    }


def create_cand_general(cand_row: pd.Series) -> Tuple[str, Dict]:
    return 'general', {
        'capacite': null_if_na(cand_row.col),
        'nb': null_if_na(cand_row.n_can),
        'nbFemmes': null_if_na(cand_row.n_can_femme),
        'clas': null_if_na(cand_row.n_clas),
        'clasFemmes': null_if_na(cand_row.n_clas_femme),
        'prop': null_if_na(cand_row.n_prop),
        'propFemmes': null_if_na(cand_row.n_prop_femme),
        'accept': null_if_na(cand_row.n_accept),
        'acceptFemmes': null_if_na(cand_row.n_accept_femme),
        'nbComp': null_if_na(cand_row.n_recrut_comp),
        'acceptDebutPP': null_if_na(cand_row.n_accept_debut_pp),
        'rangDernier': null_if_na(cand_row.rang_dernier)
    }


def create_cand_experience(cand_row: pd.Series) -> Tuple[str, Dict]:
    prop_by_cat = {
        'lg3': ['n_can_lg3', 'n_can_femme_lg3', 'n_clas_lg3', 'n_clas_femme_lg3', 'n_prop_lg3', 'n_prop_femme_lg3', 'n_accept_lg3', 'n_accept_femme_lg3'],
        'lp3': ['n_can_lp3', 'n_can_femme_lp3', 'n_clas_lp3', 'n_clas_femme_lp3', 'n_prop_lp3', 'n_prop_femme_lp3', 'n_accept_lp3', 'n_accept_femme_lp3'],
        'master': ['n_can_master', 'n_can_femme_master', 'n_clas_master', 'n_clas_femme_master', 'n_prop_master', 'n_prop_femme_master', 'n_accept_master', 'n_accept_femme_master'],
        'autre': ['n_can_autre', 'n_can_femme_autre', 'n_clas_autre', 'n_clas_femme_autre', 'n_prop_autre', 'n_prop_femme_autre', 'n_accept_autre', 'n_accept_femme_autre'],
        'noninscrit': ['n_can_noninscri', 'n_can_femme_noninscri', 'n_clas_noninscri', 'n_clas_femme_noninscri', 'n_prop_noninscri', 'n_prop_femme_noninscri', 'n_accept_noninscri', 'n_accept_femme_noninscri']
    }
    cat_keys = ['nb', 'nbFemmes', 'clas', 'clasFemme', 'prop', 'propFemmes', 'accept', 'acceptFemmes']
    component = dict((cat_key, dict((k, null_if_na(cand_row[v])) for (k,v) in zip(cat_keys, cats))) for (cat_key, cats) in prop_by_cat.items())
    return 'experience', component


def create_cand_origine(cand_row: pd.Series) -> Tuple[str, Dict]:
    prop_by_cat = {
        'etablissement': ['n_can_etab', 'n_clas_etab', 'n_prop_etab', 'n_accept_etab'],
        'academie': ['n_can_acad', 'n_clas_acad', 'n_prop_acad', 'n_accept_acad'],
        'region': ['n_can_acad_reg', 'n_clas_acad_reg', 'n_prop_acad_reg', 'n_accept_acad_reg']
    }
    cat_keys = ['nb', 'clas', 'prop', 'accept']
    component = dict(
        (cat_key, dict((k, null_if_na(cand_row[v])) for (k, v) in zip(cat_keys, cats))) for (cat_key, cats) in prop_by_cat.items())
    return 'origine', component


def create_ins_identifiants(ins_row: pd.Series) -> Tuple[str, Dict]:
    return 'identifiants', {
        'anneeCollecte': ins_row.anneeCollecte,
        'etabUai': ins_row.etabUai,
        'moisApresDip': ins_row.nbMoisApresDip,
        'insDiscId': ins_row.ins_disc
    }


def create_ins_relations(ins_row: pd.Series, sect_discs_df: pd.DataFrame,
                         mentions_df: pd.DataFrame, cache_dict: Dict = None) -> Tuple[str, Dict]:
    ins_disc = ins_row.ins_disc
    if cache_dict and ins_disc in cache_dict:
        sec_disc_ids, disc_ids, mention_ids = cache_dict[ins_disc]
    else:
        selection = sect_discs_df.loc[sect_discs_df.insDiscId == ins_disc, 'disciplineId']
        sec_disc_ids = selection.index.tolist()
        disc_ids = selection.unique().tolist()
        mention_ids = mentions_df.loc[mentions_df.secDiscId.isin(sec_disc_ids),:].index.tolist()
        if cache_dict:
            cache_dict[ins_disc] = (sec_disc_ids, disc_ids, mention_ids)

    return 'relations', {
        'academieId': ins_row.academieId,
        'regionId': ins_row.regionId,
        'secDiscIds': sec_disc_ids,
        'discIds': disc_ids,
        'mentionIds': mention_ids
    }


def create_ins_general(ins_row: pd.Series) -> Tuple[str, Dict]:
    return 'general', {
        'nbResponses': null_if_na(ins_row.nombre_de_reponses),
        'tauxReponse': null_if_na(ins_row.taux_de_reponse),
        'pbEchantillon': null_if_na(ins_row.pbEchantillon),
        'pbEchantillonRaison': null_if_na(ins_row.pbEchantillonRaison),
    }


def create_ins_emplois(ins_row: pd.Series) -> Tuple[str, Dict]:
    return 'emplois', {
        'cadreProIntermediaire': null_if_na(ins_row.emplois_cadre_ou_professions_intermediaires),
        'cadre': null_if_na(ins_row.emplois_cadre),
        'stable': null_if_na(ins_row.emplois_stables),
        'tempsPlein': null_if_na(ins_row.emplois_a_temps_plein),
        'exterieurRegionDip': null_if_na(ins_row.emplois_exterieurs_a_la_region_de_luniversite),
        'femmes': null_if_na(ins_row.femmes),
        'boursier': null_if_na(ins_row.de_diplomes_boursiers),
    }


def create_ins_salaire(ins_row: pd.Series) -> Tuple[str, Dict]:
    return 'salaire', {
        'netMedianTempsPlein': null_if_na(ins_row.salaire_net_median_des_emplois_a_temps_plein),
        'brutAnnuelEstime': null_if_na(ins_row.salaire_brut_annuel_estime),
    }


def create_ins_ref_region(ins_row: pd.Series) -> Tuple[str, Dict]:
    return 'refRegion', {
        'tauxChomageRegional': null_if_na(ins_row.emplois_cadre_ou_professions_intermediaires),
        'netQ1Regional': null_if_na(ins_row.salaire_net_mensuel_regional_1er_quartile),
        'netMedianRegional': null_if_na(ins_row.salaire_net_mensuel_median_regional),
        'netQ3Regional': null_if_na(ins_row.salaire_net_mensuel_regional_3eme_quartile),
    }

