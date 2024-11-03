import logging
from typing import List, Optional, Dict, Iterable

__all__ = ['StatSearchOptions']

LOG = logging.getLogger(__name__)


class StatSearchOptions:
    __slots__ = ['regions_filter', 'academies_filter', 'etablissements_filter', 'mentions_filter',
                 'sec_disc_filter', 'disciplines_filter', 'annee_filter', 'annee_mini_filter',
                 'annee_maxi_filter', 'mois_apres_dip_filter', 'type_stats', 'cand_details', 'inspro_details']

    def __init__(self):
        self.regions_filter: Optional[List[int]] = None
        self.academies_filter: Optional[List[int]] = None
        self.etablissements_filter: Optional[List[str]] = None
        self.mentions_filter: Optional[List[int]] = None
        self.sec_disc_filter: Optional[List[int]] = None
        self.disciplines_filter: Optional[List[int]] = None
        self.annee_filter: Optional[List[int]] = None
        self.annee_mini_filter: Optional[int] = None
        self.annee_maxi_filter: Optional[int] = None
        self.mois_apres_dip_filter: Optional[int] = None # 18 or 30 so far according to current stats data
        self.type_stats: str = "all" # all, candidatures or insertionsPro
        self.cand_details: List[str] = ['general'] # general, experience, origine, all
        self.inspro_details: List[str] = ['general'] # general, emplois, salaire, refRegion, all

    def to_dict(self) -> Dict:
        attr_vars = ((k, getattr(self, k)) for k in self.__slots__)
        attr_vars = filter(lambda d: d[1] is not None, attr_vars)
        return dict(attr_vars)

    def validate(self):
        # enforce expected type.
        # For each attribute : Nullable, Iterable, Type of attribute or if iterable,
        # of each element in attribute, list of allowed values
        expected_types = {
            'regions_filter': (True, True, int, None),
            'academies_filter': (True, True, int, None),
            'etablissements_filter': (True, True, str, None),
            'mentions_filter': (True, True, int, None),
            'sec_disc_filter': (True, True, int, None),
            'disciplines_filter': (True, True, int, None),
            'annee_filter': (True, True, int, None),
            'annee_mini_filter': (True, False, int, None),
            'annee_maxi_filter': (True, False, int, None),
            'mois_apres_dip_filter': (True, False, int, None),
            'type_stats': (False, False, str, ['all', 'candidatures', 'insertionsPro']),
            'cand_details': (False, True, str, ['all', 'general', 'experience', 'origine']),
            'inspro_details': (False, True, str, ['all', 'general', 'emplois', 'salaire', 'refRegion'])
        }

        for attr_name, (nullable, iterable, attr_type, allowed_values) in expected_types.items():
            v = getattr(self, attr_name)
            if v is None:
                if not nullable:
                    raise ValueError("Attribute %s is null and should not be." % attr_name)
                else:
                    continue
            if isinstance(v, List):
                if not iterable:
                    raise ValueError("Attribute %s is iterable and should not be." % attr_name)
                if any((not isinstance(v_elem, attr_type) for v_elem in v)):
                    raise ValueError("One of the element of iterable attribute %s has not the expected type %s."
                                     % (attr_name, str(attr_type)))
                if allowed_values is not None:
                    if any((v_elem not in allowed_values for v_elem in v)):
                        raise ValueError("One of the element of iterable attribute %s has not the expected value in %s."
                                         % (attr_name, str(allowed_values)))
            else:
                if iterable:
                    raise ValueError("Attribute %s is not iterable and should be." % attr_name)
                if not isinstance(v, attr_type):
                    raise ValueError("Attribute %s has not the expected type %s." % (attr_name, str(attr_type)))
                if allowed_values is not None and v not in allowed_values:
                    raise ValueError("Attribute %s has not the expected value in %s." % (attr_name, str(allowed_values)))

    @staticmethod
    def create_from_request_data(data: Dict):
        # create default request option
        search_opts = StatSearchOptions()
        # parse data
        filters = data.get('filters')
        if filters:
            it_var_in_out = [('regionIds', 'regions_filter'), ('academieIds', 'academies_filter'),
                             ('etablissementIds', 'etablissements_filter'), ('mentionIds', 'mentions_filter'),
                             ('secteurDisciplinaireIds', 'sec_disc_filter'), ('disciplineIds', 'disciplines_filter'),
                             ('annees', 'annee_filter')]
            dir_var_in_out = [('anneeMin', 'annee_mini_filter'), ('anneeMax', 'annee_maxi_filter'),
                              ('moisApresDiplome', 'mois_apres_dip_filter')]

            for var_in, var_out in dir_var_in_out:
                if var_in in filters:
                    setattr(search_opts, var_out, filters[var_in])

            for var_in, var_out in it_var_in_out:
                if var_in in filters:
                    v = filters[var_in]
                    if isinstance(v, List):
                        setattr(search_opts, var_out, v)
                    else:
                        setattr(search_opts, var_out, [v])

        harvest = data.get('harvest')
        if harvest:
            it_var_in_out = [('candidatureDetails', 'cand_details'), ('insertionProDetails', 'inspro_details')]
            dir_var_in_out = [('typeStats', 'type_stats')]

            for var_in, var_out in dir_var_in_out:
                if var_in in harvest:
                    setattr(search_opts, var_out, harvest[var_in])

            for var_in, var_out in it_var_in_out:
                if var_in in harvest:
                    v = harvest[var_in]
                    if isinstance(v, List):
                        setattr(search_opts, var_out, v)
                    else:
                        setattr(search_opts, var_out, [v])

        # Validate data
        search_opts.validate()
        return search_opts

    @staticmethod
    def get_search_option_template() -> Dict:
        return {
            'filters': {
                '##Description##': 'filtres de statistiques. Multiples filtres possibles, combinés en "ET"',
                'regionIds': 'Identifiant de région (int) ou tableau d\'identifiants de région. Optionnel.',
                'academieIds': 'Identifiant d\'académie (int) ou tableau d\'identifiants d\'académie. Optionnel.',
                'etablissementIds': 'Identifiant d\'université (str) ou tableau d\'identifiants d\'université. Optionnel.',
                'mentionIds': 'Identifiant de mention (int) ou tableau d\'identifiants de mention. Optionnel.',
                'secteurDisciplinaireIds': 'Identifiant de secteur disciplinaire (int) ou tableau d\'identifiants de secteurs disciplinaires. Optionnel.',
                'disciplineIds': 'Identifiant de discipline (int) ou tableau d\'identifiants de disciplines. Optionnel.',
                'annees': 'Année de collecte (int) ou tableau d\'années de collecte. Optionnel.',
                'anneeMin': 'Année minimale de collecte, inclue (int). Optionnel.',
                'anneeMax': 'Année minimale de collecte, exclue (int). Optionnel.',
                'moisApresDiplome': 'nombre de mois après obtention du diplôme (int). Ne s\'applique que pour les statistiques d\'insertion professionnelle. Optionnel.',
            },
            'harvest': {
                '##Description##': 'Choix des données retournées. Multiples options possibles.',
                'typeStats': 'Type de statistiques retournées (str). Valeur possible: {\'all\',\'candidatures\', \'insertionsPro\'}. Optionnel. Valeur par défaut : \'all\'',
                'candidatureDetails': 'Element de statistiques de candidature à retourner (str) ou tableau d\'éléments. Valeurs possibles: {\'all\', \'general\', \'experience\', \'origine\'}. Optionnel. Valeur par défaut : \'general\'',
                'insertionProDetails': 'Element de statistiques d\'insertion professionnelle à retourner (str) ou tableau d\'éléments. Valeurs possibles: {\'all\', \'general\', \'emplois\', \'salaire\', \'refRegion\'}. Optionnel. Valeur par défaut : \'general\'',
            }
        }