
def schema_multiple(values: tuple, keys: tuple) -> dict:
    diccionario = {}
    for i in range(len(values)):
        diccionario[keys[i]] = values[i]
    return diccionario


def schemas_multiples(values_list: list, keys: tuple) -> list[dict]:
    return [schema_multiple(values=values, keys=keys) for values in values_list]
