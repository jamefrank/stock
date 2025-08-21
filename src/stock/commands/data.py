import typer
from stock.utils.custom import my_update_day_data


app = typer.Typer(help="Stock data management commands")

@app.command(help="Update stock data")
def update():
    """
    Update stock data.
    """
    typer.echo("Updating stock data...")
    my_update_day_data()