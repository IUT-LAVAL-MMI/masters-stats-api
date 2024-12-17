import logging
from flask import Blueprint, Response, make_response, jsonify, request
from werkzeug.exceptions import NotFound

from controllers.http_cache_management import http_cached
from masterStats.MasterStatsManager import MasterStatsManager

__all__ = ['base_model_controller']

base_model_controller = Blueprint('base-model', __name__)

LOG = logging.getLogger(__name__)



@base_model_controller.route("/api/rest/academies", methods=['GET'])
@http_cached()
def get_academies():
    df = MasterStatsManager().academies_df
    return jsonify(df.reset_index())


@base_model_controller.route("/api/rest/etablissements", methods=['GET'])
@http_cached()
def get_etablissements():
    df = MasterStatsManager().etablissements_df
    return jsonify(df.reset_index())


@base_model_controller.route("/api/rest/secteurs-disciplinaires", methods=['GET'])
@http_cached()
def get_sect_discs():
    df = MasterStatsManager().sect_discs_df
    return jsonify(df.reset_index())


@base_model_controller.route("/api/rest/mentions", methods=['GET'])
@http_cached()
def get_mentions():
    df = MasterStatsManager().mentions_df
    return jsonify(df.reset_index())


@base_model_controller.route("/api/rest/formations", methods=['GET'])
def get_formations():
    # Potential request filters
    etab_uais = request.args.getlist('uai')
    sec_disc_ids = request.args.getlist('sdid')
    depts = request.args.getlist('dept')
    search = request.args.get('q')

    # retrieve base df and apply filter if any
    df = MasterStatsManager().formations_df
    filter = True
    if etab_uais:
        LOG.info("Add etab filter")
        if len(etab_uais) == 1:
            filter &= (df.etabUai == etab_uais[0])
        else:
            filter &= (df.etabUai.isin(etab_uais))
    if sec_disc_ids:
        LOG.info("Add secdisc filter")
        sec_disc_ids = [int(sd) for sd in sec_disc_ids]
        if len(sec_disc_ids) == 1:
            filter &= (df.secDiscId == sec_disc_ids[0])
        else:
            filter &= (df.secDiscId.isin(sec_disc_ids))
    if depts:
        LOG.info("Add dept filter")
        if len(depts) == 1:
            filter &= (df.dept == depts[0])
        else:
            filter &= (df.dept.isin(depts))
    if search:
        LOG.info("Do a text search on formations")
        ifcs = MasterStatsManager().search_formations_ifc(search)
        filter &= (df.index.isin(ifcs))

    if filter is not True:
        df = df.loc[filter, :]

    return jsonify(df.reset_index())


@base_model_controller.route("/api/rest/formations/<ifc>", methods=['GET'])
@http_cached()
def get_formation(ifc: str):
    df = MasterStatsManager().formations_df
    try:
        formation = df.loc[ifc, :]
        return jsonify(formation)
    except KeyError:
        raise NotFound('Ifc inconnu')
