import os
from Misso.services.helper import save_to_json_dict

def get_path_prefix(main="Misso"):
    p = os.getcwd()
    i = ""
    while os.path.split(p)[1] != main:
        i += "..\\"
        p = os.path.split(p)[0]
    return i, p

content = {"name":"this is a testfile"}
prefix, path = get_path_prefix()

save_to_json_dict(content, dir=prefix)