"""
Database utilities voor de WOZ Waardeloket scraper.

Gebruik dit script om de database structuur te bekijken of te initialiseren.

    python get_database_structure.py          # Toon database structuur
    python get_database_structure.py --init   # Maak lege database aan
"""

import argparse
import json

from peewee import SqliteDatabase

from run import DB_PATH, WozWaarde, PropertyModel, init_database, db


def show_database_structure():
    """Toon de database structuur en statistieken."""
    db.connect(reuse_if_open=True)

    print(f"Database: {DB_PATH}")
    print()

    # WozWaarde tabel (nieuwe structuur)
    try:
        count = WozWaarde.select().count()
        print(f"Tabel 'woz_waarden': {count} records")
        if count > 0:
            latest = (
                WozWaarde.select()
                .where(WozWaarde.laatste_woz_waarde.is_null(False))
                .order_by(WozWaarde.laatste_woz_waarde.desc())
                .limit(5)
            )
            print("  Top 5 hoogste WOZ waarden:")
            for r in latest:
                print(
                    f"    {r.straatnaam} {r.huisnummer}{r.huisletter or ''}, "
                    f"{r.postcode} {r.woonplaats} - "
                    f"EUR {r.laatste_woz_waarde:,} ({r.laatste_peildatum})"
                )
    except Exception as e:
        print(f"Tabel 'woz_waarden': niet beschikbaar ({e})")

    print()

    # PropertyModel tabel (legacy structuur)
    try:
        count = PropertyModel.select().count()
        print(f"Tabel 'propertymodel' (legacy): {count} records")
        if count > 0:
            sample = PropertyModel.select().limit(3)
            print("  Voorbeeld records:")
            for r in sample:
                print(
                    f"    {r.street} {r.house_number}, "
                    f"{r.postcode} {r.plaatsnaam} - "
                    f"ID: {r.identificatie}"
                )
    except Exception as e:
        print(f"Tabel 'propertymodel' (legacy): niet beschikbaar ({e})")

    db.close()


def main():
    parser = argparse.ArgumentParser(description="WOZ database utilities")
    parser.add_argument("--init", action="store_true", help="Initialiseer de database")
    args = parser.parse_args()

    if args.init:
        init_database()
        print("Database geinitialiseerd.")
    else:
        show_database_structure()


if __name__ == "__main__":
    main()
