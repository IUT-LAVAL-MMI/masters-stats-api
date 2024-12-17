import logging

from flask import Blueprint, jsonify, request
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
    return jsonify(df)


@base_model_controller.route("/api/rest/etablissements", methods=['GET'])
@http_cached()
def get_etablissements():
    df = MasterStatsManager().etablissements_df
    return jsonify(df)


@base_model_controller.route("/api/rest/secteurs-disciplinaires", methods=['GET'])
@http_cached()
def get_sect_discs():
    df = MasterStatsManager().sect_discs_df
    return jsonify(df)


@base_model_controller.route("/api/rest/mentions", methods=['GET'])
@http_cached()
def get_mentions():
    df = MasterStatsManager().mentions_df
    return jsonify(df)


@base_model_controller.route("/api/rest/formations", methods=['GET'])
def get_formations():
    # Potential request filters
    etab_uais = request.args.getlist('uai')
    sec_disc_ids = request.args.getlist('sdid')
    depts = request.args.getlist('dept')
    search = request.args.get('q')
    full_details = request.args.get('full-details')

    formations = MasterStatsManager().search_formations(etab_uais, sec_disc_ids, depts, search)
    if full_details and full_details.lower() not in ['no', '0', 'false']:
        return [f.to_full_dict() for f in formations]
    else:
        return [f.to_small_dict() for f in formations]


@base_model_controller.route("/api/rest/formations/<ifc>", methods=['GET'])
@http_cached()
def get_formation(ifc: str):
    df = MasterStatsManager().formations_df
    try:
        formation = df.loc[ifc, :]
        return jsonify(formation)
    except KeyError:
        raise NotFound('Ifc inconnu')
