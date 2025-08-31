from datetime import datetime
from zoneinfo import ZoneInfo
import sys
from cli.launcher import ejecutar_despertar_api, ejecutar_enviar_api, ejecutar_scraping_randstad
from cli.launcher import ejecutar_scraping_tecnoempleo, ejecutar_scraping_trabajoscom, ejecutar_etl
from utils.utils import limpiar_terminal


def distribuye_segun_hora() -> None:
    """_summary_
    distribuye_segun_hora: Llama a los diversos scripts dependiendo la hora del dia en que se encuentre
                           `despertar_api se ejecuta todas las horas, ya que todas las otras funciones
                           al final de las mismas lo ejecutan tambien.
    """
    hora_local = datetime.now(ZoneInfo("America/Caracas")).hour

    if hora_local == 18:
        ejecutar_scraping_randstad()
        return

    if hora_local == 19:
        ejecutar_scraping_tecnoempleo()
        return

    if hora_local == 20:
        ejecutar_scraping_trabajoscom()
        return

    if hora_local == 21:
        ejecutar_etl()
        return

    if hora_local == 22:
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
