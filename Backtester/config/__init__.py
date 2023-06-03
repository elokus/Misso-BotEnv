import yaml


def load_config(path="config\config.yaml")-> dict:
    with open(path) as f:
        dict = yaml.load(f, Loader=yaml.FullLoader)
    return dict

CONFIGS = load_config()