from importlib import import_module

SCRIPTS_APP = {
    "randstad": "scripts.scraping_randstad",
    "tecnoempleo": "scripts.scraping_tecnoempleo",
    "trabajoscom": "scripts.scraping_trabajoscom",
    "etl": "scripts.etl",
    "enviar_api": "scripts.enviar_api",
    "despertar_api": "scripts.despertar_api",
    "distribuye": "scripts.distribuye"
}


def ejecutar_script(modulo_path: str, *args, **kwargs):
    modulo = import_module(modulo_path)
    if hasattr(modulo, "main"):
        modulo.main(*args, **kwargs)
    else:
        raise AttributeError(f"El script '{modulo_path}' no tiene una función main()")

# def ejecutar_script(modulo_path: str):
#     modulo = import_module(modulo_path)
#     if hasattr(modulo, "main"):
#         modulo.main()
#     else:
#         raise AttributeError(f"El script '{modulo_path}' no tiene una función main()")
