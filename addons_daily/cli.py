import click
from . import addons_report


@click.group()
def entry_point():
    pass


entry_point.add_command(addons_report.main, "addons_report")

if __name__ == "__main__":
    entry_point()
