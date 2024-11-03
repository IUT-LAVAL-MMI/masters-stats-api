from functools import partial

import numpy as np
import pandas as pd
import re

__all__ = ['load_insertionspro', 'create_stats_insertionspro']

from masterStats.loading.loading_utils import secure_converter

use_ins_cols = [
    'annee', 'diplome', 'numero_de_l_etablissement', 'code_de_la_discipline',
       'situation', 'remarque', 'nombre_de_reponses', 'taux_de_reponse',
       'poids_de_la_discipline', 'taux_dinsertion', 'taux_d_emploi',
       'taux_d_emploi_salarie_en_france',
       'emplois_cadre_ou_professions_intermediaires', 'emplois_stables',
       'emplois_a_temps_plein', 'salaire_net_median_des_emplois_a_temps_plein',
       'salaire_brut_annuel_estime', 'de_diplomes_boursiers',
       'taux_de_chomage_regional', 'salaire_net_mensuel_median_regional',
       'emplois_cadre', 'emplois_exterieurs_a_la_region_de_luniversite',
       'femmes', 'salaire_net_mensuel_regional_1er_quartile',
       'salaire_net_mensuel_regional_3eme_quartile',
]

ins_dtypes = {
    'annee': np.int16,
    'diplome': str,
    'numero_de_l_etablissement': str,
    'code_de_la_discipline': str,
    'remarque': str
}

re_situation = re.compile(r'^(\d+)')


def secure_situation_extractor(value):
    m = re_situation.match(value)
    if m:
        return np.int16(m.group())
    else:
        return -1


def load_insertionspro(filepath: str) -> pd.DataFrame:
    # create specific converters
    float64_sec_converter = partial(secure_converter, dtype=np.float64, zero_on_error=False)
    insc_converters = dict((col, float64_sec_converter) for col in use_ins_cols[6:])
    insc_converters['situation'] = secure_situation_extractor
    insertionspro = pd.read_csv(filepath, sep=';',
                                usecols=use_ins_cols, dtype=ins_dtypes, converters=insc_converters)
    # remove bad rows
    bad_rows = (~insertionspro.diplome.str.upper().str.startswith('MASTER LMD')) \
               | (insertionspro.numero_de_l_etablissement.isna()) \
               | (insertionspro.code_de_la_discipline.isna()) | (insertionspro.situation == -1)
    return insertionspro.loc[~bad_rows, :].copy()


def create_stats_insertionspro(insertionspro_df: pd.DataFrame,
                               etablissements_df: pd.DataFrame,
                               academies_df: pd.DataFrame) -> pd.DataFrame:
    # drop useless columns, rename some others
    insertionspro_stats = insertionspro_df.drop(columns=['diplome']).rename(columns={
        'annee': 'anneeCollecte',
        'numero_de_l_etablissement': 'etabUai',
        'code_de_la_discipline': 'ins_disc',
        'situation': 'nbMoisApresDip',
        'remarque': 'pbEchantillonRaison'})
    # Add extra
    insertionspro_stats['pbEchantillon'] = False
    insertionspro_stats.loc[insertionspro_stats.pbEchantillonRaison.notna(), 'pbEchantillon'] = True
    # from etablissements insert academieId on etabUai (inner)
    # then from academies insert regionId on academieId (inner)
    insertionspro_stats = pd.merge(insertionspro_stats, etablissements_df.loc[:, ['academieId']],
                                   left_on='etabUai', right_on='uai', how='inner', validate='many_to_one')
    insertionspro_stats = pd.merge(insertionspro_stats, academies_df.loc[:, ['regionId']],
                                   left_on='academieId', right_on='id', how='inner', validate='many_to_one')
    return insertionspro_stats

