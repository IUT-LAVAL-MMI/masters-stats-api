import logging
from typing import List

import pandas as pd

from masterStats.MasterStatsManager import MasterStatsManager
from masterStats.search.MongoStatSearchResult import MongoStatSearchResult
from masterStats.search.StatSearchOptions import StatSearchOptions
from masterStats.search.StatSearchResult import StatSearchResult
from mongo.dao.MongoDAO import MongoDAO
from mongo.repository.CandidatureRepository import CandidatureRepository
from mongo.repository.InsertionProRepository import InsertionProRepository

__all__ = ['search_stats', 'search_candidatures', 'search_insertions_pro']

LOG = logging.getLogger(__name__)


def search_stats(search_options: StatSearchOptions) -> StatSearchResult:
    result = MongoStatSearchResult(search_options)  # StatSearchResult(search_options)
    if search_options.type_stats == 'all' or search_options.type_stats == 'candidatures':
        result.candidatures_found = mongo_search_candidatures(search_options)  # search_candidatures(search_options)
    if search_options.type_stats == 'all' or search_options.type_stats == 'insertionsPro':
        result.insertions_pro_found = mongo_search_insertions_pro(
            search_options)  # search_insertions_pro(search_options)
    return result


def search_candidatures(search_options: StatSearchOptions):
    original_cands = MasterStatsManager().stats_candidatures_df
    cands_filter = True
    if search_options.regions_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.regionId, search_options.regions_filter)
    if search_options.academies_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.academieId,
                                                               search_options.academies_filter)
    if search_options.etablissements_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.etabUai,
                                                               search_options.etablissements_filter)
    if search_options.mentions_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.mentionId, search_options.mentions_filter)
    if search_options.formations_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.formationIfc,
                                                               search_options.formations_filter)
    if search_options.sec_disc_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.secDiscId, search_options.sec_disc_filter)
    if search_options.disciplines_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.discId, search_options.disciplines_filter)
    if search_options.annee_filter:
        cands_filter &= _filter_serie_on_single_or_many_values(original_cands.anneeCollecte,
                                                               search_options.annee_filter)
    if search_options.annee_mini_filter:
        cands_filter &= original_cands.anneeCollecte >= search_options.annee_mini_filter
    if search_options.annee_maxi_filter:
        cands_filter &= original_cands.anneeCollecte < search_options.annee_maxi_filter

    return original_cands.loc[cands_filter, :] if cands_filter is not True else original_cands


def mongo_search_candidatures(search_options: StatSearchOptions):
    cands_filter = dict()
    if search_options.regions_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'regionId', search_options.regions_filter)
    if search_options.academies_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'academieId', search_options.academies_filter)
    if search_options.etablissements_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'etabUai', search_options.etablissements_filter)
    if search_options.mentions_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'mentionId', search_options.mentions_filter)
    if search_options.formations_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'formationIfc', search_options.formations_filter)
    if search_options.sec_disc_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'secDiscId', search_options.sec_disc_filter)
    if search_options.disciplines_filter:
        add_mongo_filter_on_single_or_many_values(cands_filter, 'discId', search_options.disciplines_filter)

    if search_options.annee_filter or search_options.annee_mini_filter or search_options.annee_maxi_filter:
        annee_filter = dict()
        if search_options.annee_filter:
            if len(search_options.annee_filter) == 1:
                annee_filter['$eq'] = search_options.annee_filter[0]
            else:
                annee_filter['$in'] = search_options.annee_filter
        if search_options.annee_mini_filter:
            annee_filter['$gte'] = search_options.annee_mini_filter
        if search_options.annee_maxi_filter:
            annee_filter['$lt'] = search_options.annee_maxi_filter
        cands_filter['anneeCollecte'] = annee_filter

    LOG.info("Cand filter: %s" % str(cands_filter))
    mongo_dao = MongoDAO()
    candidature_repo: CandidatureRepository = CandidatureRepository(mongo_dao.database)
    return list(candidature_repo.find_by(cands_filter))


def mongo_search_insertions_pro(search_options: StatSearchOptions):
    inspro_filter = dict()

    if search_options.regions_filter:
        add_mongo_filter_on_single_or_many_values(inspro_filter, 'regionId', search_options.regions_filter)
    if search_options.academies_filter:
        add_mongo_filter_on_single_or_many_values(inspro_filter, 'academieId', search_options.academies_filter)
    if search_options.etablissements_filter:
        add_mongo_filter_on_single_or_many_values(inspro_filter, 'etabUai', search_options.etablissements_filter)
    if search_options.mois_apres_dip_filter:
        inspro_filter['nbMoisApresDip'] = search_options.mois_apres_dip_filter
    if search_options.annee_filter or search_options.annee_mini_filter or search_options.annee_maxi_filter:
        annee_filter = dict()
        if search_options.annee_filter:
            if len(search_options.annee_filter) == 1:
                annee_filter['$eq'] = search_options.annee_filter[0]
            else:
                annee_filter['$in'] = search_options.annee_filter
        if search_options.annee_mini_filter:
            annee_filter['$gte'] = search_options.annee_mini_filter
        if search_options.annee_maxi_filter:
            annee_filter['$lt'] = search_options.annee_maxi_filter
        inspro_filter['anneeCollecte'] = annee_filter

    if search_options.mentions_filter or search_options.sec_disc_filter or search_options.disciplines_filter:
        sect_disc_df = MasterStatsManager().sect_discs_df
        disc_filter = True
        if search_options.mentions_filter:
            mentions_df = MasterStatsManager().mentions_df
            mentions_disc_test = _filter_serie_on_single_or_many_values(mentions_df.index,
                                                                        search_options.mentions_filter)
            mentions_disc_id = mentions_df.loc[mentions_disc_test, :].secDiscId.unique()
            disc_filter &= sect_disc_df.index.isin(mentions_disc_id)
        if search_options.sec_disc_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.index, search_options.sec_disc_filter)
        if search_options.disciplines_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.disciplineId,
                                                                  search_options.disciplines_filter)
        ins_disc = sect_disc_df.loc[disc_filter, 'insDiscId'].unique().tolist()
        inspro_filter['ins_disc'] = {'$in': ins_disc}

    mongo_dao = MongoDAO()
    insertionpro_repo: InsertionProRepository = InsertionProRepository(mongo_dao.database)
    return list(insertionpro_repo.find_by(inspro_filter))


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
        inspro_filter &= _filter_serie_on_single_or_many_values(original_inspro.anneeCollecte,
                                                                search_options.annee_filter)
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
            mentions_disc_test = _filter_serie_on_single_or_many_values(mentions_df.index,
                                                                        search_options.mentions_filter)
            mentions_disc_id = mentions_df.loc[mentions_disc_test, :].secDiscId.unique()
            disc_filter &= sect_disc_df.index.isin(mentions_disc_id)
        if search_options.sec_disc_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.index, search_options.sec_disc_filter)
        if search_options.disciplines_filter:
            disc_filter &= _filter_serie_on_single_or_many_values(sect_disc_df.disciplineId,
                                                                  search_options.disciplines_filter)
        ins_disc = sect_disc_df.loc[disc_filter, 'insDiscId'].unique()
        inspro_filter &= original_inspro.ins_disc.isin(ins_disc)

    return original_inspro.loc[inspro_filter, :] if inspro_filter is not True else original_inspro


def _filter_serie_on_single_or_many_values(serie: pd.Series, values: List):
    if len(values) == 1:
        return serie == values[0]
    else:
        return serie.isin(values)


def add_mongo_filter_on_single_or_many_values(filter: dict, attr_name: str, values: List):
    if len(values) == 1:
        filter[attr_name] = values[0]
    else:
        filter[attr_name] = {
            '$in': values
        }
    return filter
