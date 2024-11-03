import logging
from flask import request, Blueprint, abort, jsonify
from masterStats.search.StatSearchOptions import StatSearchOptions
from masterStats.stat_search_engine import search_stats

__all__ = ['search_stats_controller']

search_stats_controller = Blueprint('search-stats', __name__)

LOG = logging.getLogger(__name__)


@search_stats_controller.route("/api/rest/stats/search", methods=['GET'])
def get_stats_search():
    return jsonify(StatSearchOptions.get_search_option_template())


@search_stats_controller.route("/api/rest/stats/search", methods=['POST'])
def post_stats_search():
    if not request.is_json:
        abort(code=400)
    data = request.get_json(force=False)
    stat_search_options = StatSearchOptions.create_from_request_data(data)
    search_result = search_stats(stat_search_options)
    return jsonify(search_result)
