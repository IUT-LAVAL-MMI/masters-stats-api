import logging
from functools import partial
import numpy as np
import pandas as pd
from masterStats.loading.loading_utils import secure_converter

__all__ = ['load_candidates', 'create_academies', 'create_etablissements',
           'create_secteur_disciplinaires', 'create_mentions', 'create_formations',
           'create_stats_candidatures']

LOG = logging.getLogger(__name__)

use_cand_cols = [
    'session',
    'eta_uai',
    'eta_nom',
    'acad',
    'acad_lib',
    'acad_reg',
    'acad_reg_lib',
    'ifc',
    'mention',
    'parcours',
    'alternance',
    'lieux_formation',
    'discipline',
    'disci_lib',
    'secteur_disci',
    'secteur_disci_lib',
    'col',
    'n_can',
    'n_can_femme',
    'n_can_etab',
    'n_can_acad', 'n_can_acad_reg', 'n_can_lg3', 'n_can_femme_lg3',
    'n_can_lp3', 'n_can_femme_lp3', 'n_can_master', 'n_can_femme_master',
    'n_can_autre', 'n_can_femme_autre', 'n_can_noninscri',
    'n_can_femme_noninscri', 'n_clas', 'n_clas_femme', 'n_clas_etab',
    'n_clas_acad', 'n_clas_acad_reg', 'n_clas_lg3', 'n_clas_femme_lg3',
    'n_clas_lp3', 'n_clas_femme_lp3', 'n_clas_master',
    'n_clas_femme_master', 'n_clas_autre', 'n_clas_femme_autre',
    'n_clas_noninscri', 'n_clas_femme_noninscri', 'n_prop', 'n_prop_femme',
    'n_prop_etab', 'n_prop_acad', 'n_prop_acad_reg', 'n_prop_lg3',
    'n_prop_femme_lg3', 'n_prop_lp3', 'n_prop_femme_lp3', 'n_prop_master',
    'n_prop_femme_master', 'n_prop_autre', 'n_prop_femme_autre',
    'n_prop_noninscri', 'n_prop_femme_noninscri', 'n_accept',
    'n_accept_femme', 'n_accept_etab', 'n_accept_acad', 'n_accept_acad_reg',
    'n_accept_debut_pp', 'n_accept_lg3', 'n_accept_femme_lg3',
    'n_accept_lp3', 'n_accept_femme_lp3', 'n_accept_master',
    'n_accept_femme_master', 'n_accept_autre', 'n_accept_femme_autre',
    'n_accept_noninscri', 'n_accept_femme_noninscri', 'n_recrut_comp',
    'rang_dernier']

cand_dtypes = {
    'session': np.int16,
    'eta_uai': str,
    'eta_nom': str,
    'acad_lib': str,
    'acad_reg_lib': str,
    'ifc': str,
    'mention': str,
    'parcours': str,
    'alternance': bool,
    'lieux_formation': str,
    'discipline': np.int16,
    'disci_lib': str,
    'secteur_disci': np.int16,
    'secteur_disci_lib': str,
}


def secure_acad_acadreg_converter(value):
    try:
        return np.int16(value[1:])
    except ValueError as e:
        return -1


def load_candidates(filepath: str) -> pd.DataFrame:
    # Prepare specific column converters
    float64_sec_converter = partial(secure_converter, dtype=np.float64, zero_on_error=False)
    converters = dict((col, float64_sec_converter) for col in use_cand_cols[16:])
    converters['acad'] = secure_acad_acadreg_converter
    converters['acad_reg'] = secure_acad_acadreg_converter
    DF_CAND = pd.read_csv(filepath, sep=';',
                          usecols=use_cand_cols, dtype=cand_dtypes, converters=converters)
    LOG.debug('- Remove inconsistent bad row')
    bad_rows = (DF_CAND.eta_uai.isna()) | (DF_CAND.eta_nom.isna()) \
               | (DF_CAND.acad == -1) | (DF_CAND.acad_lib.isna()) | (DF_CAND.acad_reg == -1) | (
                   DF_CAND.acad_reg_lib.isna())
    DF_CAND = DF_CAND.loc[~bad_rows, :].copy()
    return DF_CAND


def create_academies(candidatures_df: pd.DataFrame) -> pd.DataFrame:
    return (candidatures_df.loc[:, ['acad', 'acad_lib', 'acad_reg', 'acad_reg_lib']]
            .drop_duplicates().rename(columns={'acad': 'id', 'acad_lib': 'nom', 'acad_reg': 'regionId',
                                               'acad_reg_lib': 'regionNom'}).set_index('id').sort_index().copy())


def create_etablissements(candidatures_df: pd.DataFrame) -> pd.DataFrame:
    return ((candidatures_df.loc[:, ['eta_uai', 'eta_nom', 'acad']].drop_duplicates()
             .rename(columns={'eta_uai': 'uai', 'eta_nom': 'nom', 'acad': 'academieId'}))
            .set_index('uai').sort_index().copy())


def create_secteur_disciplinaires(candidatures_df: pd.DataFrame, disc_mapping_df: pd.DataFrame) -> pd.DataFrame:
    sect_disc = (((candidatures_df.loc[:, ['secteur_disci', 'secteur_disci_lib', 'discipline', 'disci_lib']]
                   .drop_duplicates())
                  .rename(columns={'secteur_disci': 'id',
                                   'secteur_disci_lib': 'nom',
                                   'discipline': 'disciplineId',
                                   'disci_lib': 'disciplineNom'}))
                 .set_index('id').sort_index().copy())
    return pd.merge(sect_disc, disc_mapping_df, on='id', how='inner', validate='one_to_one')


def create_mentions(candidatures_df: pd.DataFrame) -> pd.DataFrame:
    mentions = (candidatures_df.loc[:, ['mention', 'secteur_disci']].drop_duplicates()
                .rename(columns={'mention': 'nom', 'secteur_disci': 'secDiscId'})
                .sort_values('nom').reset_index(drop=True).copy())
    mentions.index.name = 'id'
    return mentions


def create_formations(candidatures_df: pd.DataFrame, mentions_df: pd.DataFrame) -> pd.DataFrame:
    formations = (candidatures_df.loc[:, ['ifc', 'parcours', 'alternance', 'lieux_formation',
                                        'eta_uai', 'mention']]
                  .drop_duplicates()
                  .rename(columns={'lieux_formation': 'lieux', 'eta_uai': 'etabUai'}))
    # merge formation with mentionId
    formations = pd.merge(formations,
                          mentions_df.reset_index().rename(columns={'id': 'mentionId', 'nom': 'mention'}),
                          on='mention', how='left', validate='many_to_one')
    # remove mention, set ifc as index and sort by ifs
    formations = formations.drop(columns=['mention']).set_index('ifc').sort_index()
    # extract and add ville and depts
    villes_depts = formations.lieux.str.extract(r'-\s+(?P<ville>[A-Z\s]+[A-Z])\s+\((?P<dept>\d+)\)\s*$')
    formations = pd.concat([formations, villes_depts], axis=1)
    return formations


def create_stats_candidatures(candidatures_df: pd.DataFrame, formations_df: pd.DataFrame) -> pd.DataFrame:
    candidatures_stats = (candidatures_df.drop(columns=['eta_nom', 'acad_lib', 'acad_reg_lib', 'parcours',
                                                       'alternance', 'lieux_formation',
                                                       'disci_lib', 'secteur_disci_lib'])
    .rename(columns={
        'session': 'anneeCollecte',
        'eta_uai': 'etabUai',
        'ifc': 'formationIfc',
        'acad': 'academieId',
        'acad_reg': 'regionId',
        'discipline': 'discId',
        'secteur_disci': 'secDiscId'
    }))
    candidatures_stats = pd.merge(candidatures_stats, formations_df.loc[:, ['mentionId']],
                                  left_on='formationIfc', right_on='ifc',
                                  how='inner', validate='many_to_one')
    return candidatures_stats
