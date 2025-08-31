from rich.console import Console
from rich.table import Table
from utils.utils import limpiar_terminal

console = Console()


def menu_principal():
    limpiar_terminal()
    opciones = [
        ("1", "🕸️ Scraping Randstad"),
        ("2", "🕸️ Scraping Tecnoempleo"),
        ("3", "🕸️ Scraping Trabajos.com"),
        ("4", "📂 Procesar data externa ETL"),
        ("5", "📡 Envío de datos a API"),
        ("6", "📡 Mantener despierta API"),
        ("7", "📡 Distribuye según horario")
        ("8", "❌ Salir")
    ]

    table = Table(title="📋 Menú Principal", show_header=False, padding=(0, 1), style="cyan", border_style="blue")
    for clave, texto in opciones:
        table.add_row(f"[bold yellow]{clave}[/]", texto)
    console.print(table)
