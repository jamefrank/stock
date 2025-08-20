import typer

from stock.commands import data
from stock.commands import analyze


app = typer.Typer(
    name = "stock",
    help = "A command line tool for stock",
    epilog="Welcome to the stock cli"
)

app.add_typer(data.app, name="data", help="Stock data management commands")
app.add_typer(analyze.app, name="analyze", help="Stock analyze commands")

if __name__ == "__main__":
    app()