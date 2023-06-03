


class Wave:
    """get next wave for a position, return wave dict for saving position, create from wave dict after restore"""
    def __init__(self, wave_dict: dict):
        self.waves = wave_dict

    def __repr__(self):
        return self.waves


