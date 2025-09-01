from datetime import datetime
from zoneinfo import ZoneInfo
import sys
from cli.launcher import ejecutar_despertar_api, ejecutar_enviar_api, ejecutar_scraping_randstad
from cli.launcher import ejecutar_scraping_tecnoempleo, ejecutar_scraping_trabajoscom, ejecutar_etl
from utils.utils import limpiar_terminal
from scripts_registry import ejecutar_script, SCRIPTS_APP


def distribuye_segun_hora() -> None:
    """_summary_
    distribuye_segun_hora: Llama a los diversos scripts dependiendo la hora del dia en que se encuentre
                           `despertar_api se ejecuta todas las horas, ya que todas las otras funciones
                           al final de las mismas lo ejecutan tambien.
    """
    hora_local = datetime.now(ZoneInfo("America/Caracas")).hour

    if hora_local == 18:
        ejecutar_scraping_randstad()
        ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=1, limpiar=False, segundos=3)
        ejecutar_scraping_tecnoempleo()
        ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=1, limpiar=False, segundos=3)
        ejecutar_scraping_trabajoscom()
        ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=1, limpiar=False, segundos=3)
        ejecutar_etl()
        ejecutar_script(SCRIPTS_APP["despertar_api"], maximo_intentos=1, limpiar=False, segundos=3)
        ejecutar_enviar_api()
        return

    ejecutar_despertar_api()
    return


def main() -> None:
    """_summary_
    main: Punto de entrada al script `distribuye`...  Casi nunca usado durante el desarrollo, pero en producción
          será el UNICO punto de entrada que se utilice y que desde acá, según sea la hora del dia, se canalicen los
          distintos scripts
    """
    limpiar_terminal()
    distribuye_segun_hora()
    sys.exit()
