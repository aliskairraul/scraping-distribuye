import os
import json
from pathlib import Path
import logging
import unicodedata


def quitar_acentos(texto):
    return ''.join(
        c for c in unicodedata.normalize('NFD', texto)
        if unicodedata.category(c) != 'Mn'
    )


def limpiar_terminal():
    """
        limpiar_terminal: como su nombre lo indica, limpia la terminal
    """
    os.system('cls' if os.name == 'nt' else 'clear')


def guardar_json(archivo: dict, ruta: Path) -> None:
    with ruta.open("w", encoding="utf-8") as f:
        json.dump(archivo, f, indent=4)


def borrar_archivos(archivos: list, carpeta: str, logger: logging) -> None:
    """_summary_

    Args:
        archivos (list): Lista de archivos a borra
        carpeta (str): Carpeta donde se encuentran ubicados
        logger (logging): Logger de la app para seguimiento
    """
    for archivo in archivos:
        ruta = os.path.join(carpeta, archivo)
        try:
            os.remove(ruta)
            logger.info(f"Archivo de log {ruta} eliminado")
        except TimeoutError:
            logger.error(f"Ocurrio error tratando de borrar {ruta}")
