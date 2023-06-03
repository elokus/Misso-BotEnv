import uuid

class CustomId:
    @staticmethod
    def generate(caller_id: str, ref_id: str=None, **kwargs):
        import uuid
        _id = [caller_id, str(uuid.uuid4())]
        if ref_id:
            _id.append(ref_id)
        _spez = {key: str(arg) for key, arg in kwargs.items()}
        if len(_spez) > 0:
            _id.append(CustomId.parse_from_list(_spez, list_sep="-"))
        return CustomId.parse_from_list(_id)

    @staticmethod
    def generate_params(subaccount: str, symbol: str, **kwargs):
        return {"clientId": CustomId.generate(subaccount, symbol, **kwargs)}

    @staticmethod
    def add_client_id(params: dict, subaccount: str, symbol: str, **kwargs):
        params = {} if params is None else params
        params["clientId"] = CustomId.generate(subaccount, symbol, **kwargs)
        return params

    @staticmethod
    def _format_string(element: list, list_sep: str="_", dict_sep: str="-", dict_elem_sep: str="%"):
        if isinstance(element, list):
            e = list_sep.join(element)
        elif isinstance(element, dict):
            _e = [dict_elem_sep.join([str(k), str(v)]) for k, v in element.items()]
            e = dict_sep.join(_e)
        else:
            print(f"HELPER:PY   _format_string e is neither list nor dict: {element}")
        return e

    @staticmethod
    def parse_from_list(id_container: list, list_sep: str="_", dict_sep: str="-", dict_elem_sep: str="%"):
        _id = []
        if isinstance(id_container, list):
            for id in id_container:
                if isinstance(id, list):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                elif isinstance(id, dict):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                else:
                    e = str(id)
                _id.append(e)
        elif isinstance(id_container, dict):
            _id = {}
            for key, id in id_container.items():
                if isinstance(id, list):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                elif isinstance(id, dict):
                    e = CustomId._format_string(id, list_sep, dict_sep, dict_elem_sep)
                else:
                    e = str(id)
                _id[key] = e
        return CustomId._format_string(_id, list_sep, dict_sep, dict_elem_sep)

    @staticmethod
    def update(old_id: str, ref_id: str, update_spez: bool=False, **kwargs):
        if len(old_id.split("_")) >= 3:
            src_id, uid, _ref_id, *_spez = old_id.split("_")
            ref_id = _ref_id if ref_id is None else ref_id
            if update_spez:
                _d = {e.split("%")[0] : e.split("%")[1] for e in _spez.split("-") if "%" in e}
                for k, v in kwargs.items():
                    _d[k] = str(v)
        else:
            src_id, uid = old_id.split("_")
            _d = {k: str(v) for k, v in kwargs.items()}
        new_id = [src_id, uid, ref_id, _d] if len(_d) > 0 else [src_id, uid, ref_id]
        if len(_d) > 0:
            new_id.append(_d)
        return CustomId.parse_from_list(new_id)

    @staticmethod
    def parse_from_string(client_id):
        src, uid, ref, *spez = client_id.split("_")
        if len(spez) > 0:
            spez = {e.split("%")[0]: e.split("%")[1] for e in spez[0].split("-")}
        return {
            "src_id": src,
            "uid": uid,
            "ref_id": ref,
            "spez": spez
        }
    @staticmethod
    def parse_from_dict(d: dict):
        return CustomId.parse_from_list([v for v in d.values()])