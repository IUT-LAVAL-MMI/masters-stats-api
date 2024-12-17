import atexit
import logging
from argparse import ArgumentParser
from flask import Flask
from flask_cors import CORS
from werkzeug.middleware.proxy_fix import ProxyFix

from controllers.baseModelController import base_model_controller
from controllers.errorHandler import error_handler
from controllers.searchStatsController import search_stats_controller
from jsonProcessing.ExtendedJsonProvider import ExtendedJsonProvider
from masterStats.MasterStatsManager import MasterStatsManager
from mongo.dao.MongoDAO import MongoDAO
from utils.loggingUtils import configure_logging

LOG = logging.getLogger(__name__)


def setup_argument_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Teacher AI Support Server")
    parser.add_argument('-c', '--config', help="Configuration file location (default: ./config.py)",
                        metavar='<configuration file>', type=str, default='./config.py')
    parser.add_argument('-l', '--log-level', help="Level of logger", metavar='<logging level>', type=str,
                        default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'FATAL'])
    return parser


def setup_argument_parser() -> ArgumentParser:
    parser = ArgumentParser(description="Teacher AI Support Server")
    parser.add_argument('-c', '--config', help="Configuration file location (default: ./config.py)",
                        metavar='<configuration file>', type=str, default='./config.py')
    parser.add_argument('--variable-description',
                        help="Variable description CSV file (default: ./taxonomy_metrics.csv)",
                        metavar='<variable description file>', type=str, default='./taxonomy_metrics.csv')
    parser.add_argument('--iaModels-path',
                        help="Model storage path (default: /tmp/iaModels)",
                        metavar='<model folder path>', type=str, default='/tmp/iaModels')
    parser.add_argument('-l', '--log-level', help="Level of logger", metavar='<logging level>', type=str,
                        default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'FATAL'])
    return parser


def create_server_apps(config_file_path: str, log_level: str) -> Flask:
    # Flask application
    LOG.info("Init flask app with configuration file {}..."
             .format( config_file_path))
    app: Flask = Flask(__name__)
    app.config.from_pyfile(config_file_path)
    app.config['LOG_LEVEL'] = log_level  # Will be used by subprocesses to configure their logging

    # Proxy settings
    app.wsgi_app = ProxyFix(
        app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1
    )

    # CORS setup for Flask and socket.io
    if app.config.get('ENABLE_CORS', False):
        app.logger.warning("ENABLE CORS")
        cors = CORS(app, resources={r"/api/*": {
            "origins": ["*"]
        }})

    # Custom JSON Provider for the app
    app.json = ExtendedJsonProvider(app)

    # MONGO Access setup
    app.logger.info("Open MongoDAO")
    mongo_dao = MongoDAO(MongoDAO.compute_dao_options_from_app(app.config))
    mongo_dao.open()
    app.logger.info("Mongo init index")
    mongo_dao.init_indexes()

    # Stats Singleton service setup
    app.logger.info("Stats Manager setup")
    MasterStatsManager(app.config).build_stats()
    app.logger.info("Build mongo cache if required")
    MasterStatsManager().build_mongo_cache()

    # Mongo text model


    # REST Controllers setup
    app.logger.info("REST Controller setup")
    app.register_blueprint(error_handler)
    app.register_blueprint(base_model_controller)
    app.register_blueprint(search_stats_controller)

    return app


def server_cleanup(mongo_dao: MongoDAO):
    LOG.info("Shutdown cleanup")
    LOG.info("Close mongo DAO")
    mongo_dao.close()


def main(log_level: str = 'INFO', config: str = './config.py'):
    # Logging configuration
    configure_logging(log_level)
    # Create and configure apps
    server_app = create_server_apps(config, log_level)
    # Register cleanup function at shutdown
    atexit.register(server_cleanup, MongoDAO())
    return server_app


def start_dev_server(app: Flask):
    app.logger.info("Start Master-stats-api Support Server")
    host = app.config.get('SERVER_HOST_DEV', '127.0.0.1')
    port = app.config.get('SERVER_PORT_DEV', 5000)
    debug = app.config.get('DEBUG', False)
    app.logger.info("Start on host {} and port {}".format(host, port))
    if debug:
        app.logger.warning('DEBUG mode enabled')
    app.run(host=host, port=port, debug=debug)


if __name__ == '__main__':
    # Parse application arguments
    arg_parser = setup_argument_parser()
    args = arg_parser.parse_args()
    app = main(args.log_level, args.config)
    # Start server
    start_dev_server(app)
