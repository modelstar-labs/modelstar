
def py_to_snow_type(py_type: str) -> str:
    py_to_snow = {'int': 'NUMBER', 'str': 'STRING', 'float': 'FLOAT', 'bool': 'BOOL',
                  'bytes': 'BINARY', 'list': 'ARRAY', 'dict': 'OBJECT', 'DataFrame': 'STRING'}

    if py_type in py_to_snow:
        snow_type = py_to_snow[py_type]
    else:
        raise ValueError(f'Un-recognized python type `{py_type}`')

    return snow_type
