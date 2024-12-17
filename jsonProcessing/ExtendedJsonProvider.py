import json
import logging
import typing as t
import numpy as np
import pandas as pd
from flask.json.provider import JSONProvider
from flask.sansio.app import App

from masterStats.search.StatSearchResult import StatSearchResult

__all__ = ['ExtendedJsonProvider']

LOG = logging.getLogger(__name__)


class ExtendedJsonProvider(JSONProvider):

    def __init__(self, app: App) -> None:
        super().__init__(app)

    def dumps(self, obj: t.Any, **kwargs: t.Any) -> str:
        kwargs['ensure_ascii'] = False
        kwargs['indent'] = None
        if isinstance(obj, pd.DataFrame):
            LOG.debug('Dumps DataFrame')
            return obj.to_json(orient='records', force_ascii=False, compression=None, indent=None)
        elif isinstance(obj, StatSearchResult):
            LOG.debug('Dumps StatSearchResult')
            return json.dumps(obj.to_dict(), **kwargs)
        elif isinstance(obj, np.integer):
            LOG.debug('Dumps np.interger')
            return json.dumps(int(obj), **kwargs)
        elif isinstance(obj, np.floating):
            LOG.debug('Dumps np.floating')
            if np.isnan(obj):
                return json.dumps(None, **kwargs)
            else:
                return json.dumps(float(obj), **kwargs)
        elif isinstance(obj, pd.Series):
            return obj.to_json(orient='index', force_ascii=False, compression=None, indent=None)
        else:
            return json.dumps(obj, **kwargs)

    def loads(self, s: str | bytes, **kwargs: t.Any) -> t.Any:
        return json.loads(s, **kwargs)
