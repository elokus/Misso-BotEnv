import importlib

class Meta(type):
    def __new__(cls, name, base, dct, **kwargs):
        assert "base_modul" in kwargs, "please define base_modul for metaclass"
        assert "bases" in kwargs, "please define list of bases for metaclass"
        modul = importlib.import_module(kwargs["base_modul"])
        bases = kwargs["bases"]
        _bases = []
        for _base in bases:
            if hasattr(modul, _base):
                _bases.append(getattr(modul, _base))
        return type(name, tuple(_bases), dct)

class iMeta(type):
    def __new__(cls, name, base, dct, **kwargs):
        modul = importlib.import_module(kwargs["modul"])
        base_name = kwargs["base_name"]
        if hasattr(modul, base_name):
            _base = (getattr(modul, base_name), )
            return type(name, _base, dct)

class BaseDC(object):
    def __post_init__(self):
        pass