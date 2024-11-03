from typing import List
import pandas as pd
from masterStats.MasterStatsManager import MasterStatsManager
from masterStats.search.StatSearchOptions import StatSearchOptions
from masterStats.search.StatSearchResult import StatSearchResult

__all__ = ['search_stats', 'search_candidatures', 'search_insertions_pro']


def search_stats(search_options: StatSearchOptions) -> StatSearchResult:
    result = StatSearchResult(search_options)
    if search_options.type_stats == 'all' or search_options.type_stats == 'candidatures':
        result.candidatures_found = search_candidatures(search_options)
    if search_options.type_stats == 'all' or search_options.type_stats == 'insertionsPro':
        result.insertions_pro_found = search_insertions_pro(search_options)
    return result


def search_candidatures(search_options: StatSearchOptions):
    original_cands = MasterStatsManager().stats_candidatures_df
    cands_filter = True
    if search_options.regions_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.regionId, search_options.regions_filter)
    if search_options.academies_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.academieId, search_options.academies_filter)
    if search_options.etablissements_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.etabUai, search_options.etablissements_filter)
    if search_options.mentions_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.mentionId, search_options.mentions_filter)
    if search_options.sec_disc_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.secDiscId, search_options.sec_disc_filter)
    if search_options.disciplines_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.discId, search_options.disciplines_filter)
    if search_options.annee_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.anneeCollecte, search_options.annee_filter)
    if search_options.annee_mini_filter:
        cands_filter &= original_cands.anneeCollecte >= search_options.annee_mini_filter
    if search_options.annee_maxi_filter:
        cands_filter &= original_cands.anneeCollecte < search_options.annee_maxi_filter

    return original_cands.loc[cands_filter, :] if cands_filter is not True else original_cands


def search_insertions_pro(search_options: StatSearchOptions):
    original_inspro = MasterStatsManager().stats_insertionspro_df
    inspro_filter = True
    if search_options.regions_filter:
        inspro_filter &= _filter_serie_on_single_or_many_values(original_inspro.regionId, search_options.regions_filter)
    if search_options.academies_filter:
        inspro_filter &= _filter_serie_on_single_or_many_values(original_inspro.academieId,
                                                               search_options.academies_filter)
    if search_options.etablissements_filter:
        inspro_filter &= _filter_serie_on_single_or_many_values(original_inspro.etabUai,
                                                               search_options.etablissements_filter)
    if search_options.annee_filter:
        inspro_filter &= _filter_serie_on_single_or_many_values(original_inspro.anneeCollecte, search_options.annee_filter)
    if search_options.annee_mini_filter:
        inspro_filter &= original_inspro.anneeCollecte >= search_options.annee_mini_filter
    if search_options.annee_maxi_filter:
        inspro_filter &= original_inspro.anneeCollecte < search_options.annee_maxi_filter
    if search_options.mois_apres_dip_filter:
        inspro_filter &= original_inspro.nbMoisApresDip == search_options.mois_apres_dip_filter

    if search_options.mentions_filter or search_options.sec_disc_filter or search_options.disciplines_filter:
        sect_disc_df = MasterStatsManager().sect_discs_df
        disc_filter = True
        if search_options.mentions_filter:
            mentions_df = MasterStatsManager().mentions_df
            mentions_disc_test = _filter_serie_on_single_or_many_values(mentions_df.index, search_options.mentions_filter)
            mentions_disc_id = mentions_df.loc[mentions_disc_test,:].secDiscId.unique()
            disc_filter &= sect_disc_df.index.isin(mentions_disc_id)
        if search_options.sec_disc_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.index, search_options.sec_disc_filter)
        if search_options.disciplines_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.disciplineId, search_options.disciplines_filter)
        ins_disc = sect_disc_df.loc[disc_filter, 'insDiscId'].unique()
        inspro_filter &= original_inspro.ins_disc.isin(ins_disc)

    return original_inspro.loc[inspro_filter, :] if inspro_filter is not True else original_inspro


def _filter_serie_on_single_or_many_values(serie: pd.Series, values: List):
    if len(values) == 1:
        return serie == values[0]
    else:
        return serie.isin(values)