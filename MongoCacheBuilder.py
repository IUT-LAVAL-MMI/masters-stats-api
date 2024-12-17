import errno
import logging
import os
import types

from MasterStatsAPI import setup_argument_parser
from masterStats.MasterStatsManager import MasterStatsManager
from mongo.dao.MongoDAO import MongoDAO
from utils.loggingUtils import configure_logging

LOG = logging.getLogger(__name__)


def read_py_file_config(filename: str | os.PathLike[str], silent: bool = False) -> dict:
    """
    Read a config .py file as a dict
    :param filename: the filepath of the config file
    :param silent: if set to True, will fail in silent
    :return: the dict of config
    """
    d = types.ModuleType("config")
    d.__file__ = filename
    try:
        with open(filename, mode="rb") as config_file:
            exec(compile(config_file.read(), filename, "exec"), d.__dict__)
    except OSError as e:
        if silent and e.errno in (errno.ENOENT, errno.EISDIR, errno.ENOTDIR):
            return dict()
        e.strerror = f"Unable to load configuration file ({e.strerror})"
        raise
    # Create a proper dict with all key/value of the object that are in upper case
    config = dict()
    for key in dir(d):
        if key.isupper():
            config[key] = getattr(d, key)
    return config


def main(log_level: str = 'INFO', config: str = './config.py'):
    # Logging configuration
    configure_logging(log_level)

    # read the configuration as a dict
    LOG.info("Load configuration")
    config = read_py_file_config(args.config)
    LOG.info("Stats Manager setup")
    master_stats_mgr = MasterStatsManager(config)
    LOG.info("Load full stats")
    master_stats_mgr.build_full_stats()
    LOG.info("(Re-)Build Mongo cache")
    with MongoDAO(MongoDAO.compute_dao_options_from_app(config)) as mongo_dao:
        LOG.info("Mongo init index")
        mongo_dao.init_indexes()
        master_stats_mgr.build_mongo_cache(clear_col=True)
    LOG.info("Cache building done")


if __name__ == '__main__':
    # Parse application arguments
    arg_parser = setup_argument_parser()
    args = arg_parser.parse_args()
    main(args.log_level, args.config)
