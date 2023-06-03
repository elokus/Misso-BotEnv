import yaml
import Misso.services.helper as ph
import os
DIR = os.path.dirname(os.path.realpath(__file__))
API_CONFIG = ph.yaml_to_dict(DIR + "/exchange_config.yaml")