"""
WOZ Waardeloket Scraper

Scrapt WOZ-waarden van Nederlandse panden via de publieke APIs:
- PDOK Locatieserver (adres zoeken)
- Kadaster WOZ Waardeloket API (WOZ waarden ophalen)
- Legacy WFS endpoint (fallback/bulk scraping)

Gebruik:
    python run.py --postcode 1017AB
    python run.py --postcode 1017AB --huisnummer 10
    python run.py --adres "Dam 1 Amsterdam"
    python run.py --postcodes-file postcodes.txt
    python run.py --bulk --from-id 0 --to-id 100000
"""

import argparse
import csv
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
from peewee import (
    CharField,
    IntegerField,
    Model,
    SqliteDatabase,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

DB_PATH = "woz_waarden.db"
db = SqliteDatabase(DB_PATH)

# --- API endpoints ---
PDOK_SUGGEST_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/suggest"
PDOK_FREE_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/free"
PDOK_LOOKUP_URL = "https://api.pdok.nl/bzk/locatieserver/search/v3_1/lookup"
WOZ_API_URL = "https://api.kadaster.nl/lvwoz/wozwaardeloket-api/v1/wozwaarde/nummeraanduiding"
# Legacy WFS endpoint (for bulk scraping by object ID range)
WOZ_WFS_URL = "https://www.wozwaardeloket.nl/woz-proxy/wozloket"
WOZ_SESSION_URL = "https://www.wozwaardeloket.nl/index.jsp?a=1&accept=true&"

REQUEST_TIMEOUT = 15
RATE_LIMIT_DELAY = 0.5  # seconds between requests


# --- Database models ---

class BaseModel(Model):
    class Meta:
        database = db


class WozWaarde(BaseModel):
    nummeraanduiding_id = CharField(unique=True)
    woz_object_nummer = CharField(null=True)
    straatnaam = CharField(null=True)
    huisnummer = IntegerField(null=True)
    huisletter = CharField(null=True)
    huisnummer_toevoeging = CharField(null=True)
    postcode = CharField(null=True, index=True)
    woonplaats = CharField(null=True)
    gebruiksdoel = CharField(null=True)
    oppervlakte = IntegerField(null=True)
    bouwjaar = IntegerField(null=True)
    # WOZ waarden per peiljaar (opgeslagen als JSON string met alle jaren)
    woz_waarden_json = CharField(null=True)
    # Meest recente WOZ waarde apart voor makkelijk filteren
    laatste_peildatum = CharField(null=True)
    laatste_woz_waarde = IntegerField(null=True)

    class Meta:
        table_name = "woz_waarden"


# Legacy model for backwards compatibility with old database
class PropertyModel(BaseModel):
    identificatie = CharField(unique=True)
    house_number = CharField(null=True)
    house_number_ext = CharField(null=True)
    postcode = CharField(null=True)
    plaatsnaam = CharField(null=True)
    street = CharField(null=True)
    price_2015 = CharField(null=True)
    price_2016 = CharField(null=True)
    price_2017 = CharField(null=True)
    bouwjaar = CharField(null=True)
    gebruiksdoel = CharField(null=True)
    oppervlakte = CharField(null=True)

    class Meta:
        table_name = "propertymodel"


def init_database():
    db.connect(reuse_if_open=True)
    db.create_tables([WozWaarde, PropertyModel])
    logger.info("Database geinitialiseerd: %s", DB_PATH)


# --- API Client ---

class WozScraper:
    def __init__(self, delay=RATE_LIMIT_DELAY):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "application/json",
        })
        self.delay = delay
        self._wfs_session_initialized = False

    def _rate_limit(self):
        if self.delay > 0:
            time.sleep(self.delay)

    # --- Methode 1: Moderne API (PDOK + Kadaster) ---

    def zoek_adressen_postcode(self, postcode, huisnummer=None):
        """Zoek adressen op postcode via PDOK Locatieserver."""
        query = f"postcode:{postcode.replace(' ', '')}"
        if huisnummer:
            query += f" and huisnummer:{huisnummer}"

        params = {
            "q": query,
            "fq": "type:adres",
            "fl": (
                "id,weergavenaam,straatnaam,huisnummer,huisletter,"
                "huisnummertoevoeging,postcode,woonplaatsnaam,"
                "gemeentenaam,nummeraanduiding_id"
            ),
            "rows": 100,
        }

        try:
            resp = self.session.get(PDOK_FREE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            logger.info(
                "PDOK: %d adressen gevonden voor postcode %s%s",
                len(docs), postcode,
                f" huisnummer {huisnummer}" if huisnummer else "",
            )
            return docs
        except requests.RequestException as e:
            logger.error("PDOK request mislukt: %s", e)
            return []

    def zoek_adressen_vrij(self, adres_tekst):
        """Zoek adressen op vrije tekst via PDOK Locatieserver."""
        params = {
            "q": adres_tekst,
            "fq": "type:adres",
            "fl": (
                "id,weergavenaam,straatnaam,huisnummer,huisletter,"
                "huisnummertoevoeging,postcode,woonplaatsnaam,"
                "gemeentenaam,nummeraanduiding_id"
            ),
            "rows": 100,
        }

        try:
            resp = self.session.get(PDOK_FREE_URL, params=params, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()
            data = resp.json()
            docs = data.get("response", {}).get("docs", [])
            logger.info("PDOK: %d adressen gevonden voor '%s'", len(docs), adres_tekst)
            return docs
        except requests.RequestException as e:
            logger.error("PDOK request mislukt: %s", e)
            return []

    def haal_woz_waarde(self, nummeraanduiding_id):
        """Haal WOZ waarden op voor een nummeraanduiding ID via Kadaster API."""
        url = f"{WOZ_API_URL}/{nummeraanduiding_id}"
        self._rate_limit()

        try:
            resp = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if resp.status_code == 404:
                logger.warning("Geen WOZ data voor nummeraanduiding %s", nummeraanduiding_id)
                return None
            resp.raise_for_status()
            data = resp.json()
            return data
        except requests.RequestException as e:
            logger.error("WOZ API request mislukt voor %s: %s", nummeraanduiding_id, e)
            return None

    def scrape_adres_en_sla_op(self, adres_doc):
        """Verwerk een PDOK adres-document en sla WOZ data op in de database."""
        nummeraanduiding_id = adres_doc.get("nummeraanduiding_id")
        if not nummeraanduiding_id:
            logger.warning("Geen nummeraanduiding_id in adres: %s", adres_doc.get("weergavenaam"))
            return False

        # Check of we dit adres al hebben
        if WozWaarde.select().where(WozWaarde.nummeraanduiding_id == nummeraanduiding_id).exists():
            logger.debug("Al in database: %s", adres_doc.get("weergavenaam"))
            return False

        woz_data = self.haal_woz_waarde(nummeraanduiding_id)
        if not woz_data:
            return False

        return self._sla_woz_op(adres_doc, woz_data)

    def _sla_woz_op(self, adres_doc, woz_data):
        """Parse WOZ API response en sla op in database."""
        nummeraanduiding_id = adres_doc.get("nummeraanduiding_id", "")

        # Extract WOZ object info
        woz_object = woz_data.get("wozObject", {})
        woz_waarden = woz_data.get("wozWaarden", [])

        # Zoek meest recente WOZ waarde
        laatste_peildatum = None
        laatste_woz_waarde = None
        waarden_lijst = []
        for w in woz_waarden:
            peildatum = w.get("peildatum", "")
            waarde = w.get("vastgesteldeWaarde")
            waarden_lijst.append({"peildatum": peildatum, "waarde": waarde})
            if waarde is not None:
                if laatste_peildatum is None or peildatum > laatste_peildatum:
                    laatste_peildatum = peildatum
                    laatste_woz_waarde = waarde

        # Parse bouwjaar en oppervlakte uit adres_doc of woz_object
        bouwjaar = None
        oppervlakte = None
        gebruiksdoel = None

        try:
            record = WozWaarde.create(
                nummeraanduiding_id=nummeraanduiding_id,
                woz_object_nummer=woz_object.get("wozObjectNummer"),
                straatnaam=adres_doc.get("straatnaam"),
                huisnummer=adres_doc.get("huisnummer"),
                huisletter=adres_doc.get("huisletter"),
                huisnummer_toevoeging=adres_doc.get("huisnummertoevoeging"),
                postcode=adres_doc.get("postcode"),
                woonplaats=adres_doc.get("woonplaatsnaam"),
                gebruiksdoel=gebruiksdoel,
                oppervlakte=oppervlakte,
                bouwjaar=bouwjaar,
                woz_waarden_json=json.dumps(waarden_lijst),
                laatste_peildatum=laatste_peildatum,
                laatste_woz_waarde=laatste_woz_waarde,
            )
            logger.info(
                "Opgeslagen: %s %s%s, %s %s - WOZ %s: EUR %s",
                adres_doc.get("straatnaam", ""),
                adres_doc.get("huisnummer", ""),
                adres_doc.get("huisletter", "") or "",
                adres_doc.get("postcode", ""),
                adres_doc.get("woonplaatsnaam", ""),
                laatste_peildatum or "?",
                f"{laatste_woz_waarde:,}" if laatste_woz_waarde else "?",
            )
            return True
        except Exception as e:
            logger.error("Database fout: %s", e)
            return False

    # --- Methode 2: Legacy WFS endpoint (bulk scraping) ---

    def _init_wfs_session(self):
        """Initialiseer WFS sessie met cookie."""
        if self._wfs_session_initialized:
            return
        try:
            wfs_session = requests.Session()
            wfs_session.get(WOZ_SESSION_URL, timeout=REQUEST_TIMEOUT)
            self._wfs_cookies = wfs_session.cookies
            self._wfs_session_initialized = True
            logger.info("WFS sessie geinitialiseerd")
        except requests.RequestException as e:
            logger.error("WFS sessie initialisatie mislukt: %s", e)

    def scrape_wfs_id_range(self, from_id, to_id):
        """Scrape panden via legacy WFS endpoint op basis van object ID range."""
        from_id_str = f"{from_id:012d}"
        to_id_str = f"{to_id:012d}"

        # Maak voor elke request een nieuwe sessie (voorkomt bans)
        s = requests.Session()
        try:
            s.get(WOZ_SESSION_URL, timeout=REQUEST_TIMEOUT)
        except requests.RequestException as e:
            logger.error("WFS sessie fout: %s", e)
            return None

        xml_request = f"""
        <wfs:GetFeature
            xmlns:wfs="http://www.opengis.net/wfs" service="WFS" version="1.1.0"
            xsi:schemaLocation="http://www.opengis.net/wfs http://schemas.opengis.net/wfs/1.1.0/wfs.xsd"
            xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">
            <wfs:Query typeName="wozloket:woz_woz_object" srsName="EPSG:28992"
                xmlns:WozViewer="http://WozViewer.geonovum.nl"
                xmlns:ogc="http://www.opengis.net/ogc">
                <ogc:Filter xmlns:ogc="http://www.opengis.net/ogc">
                    <ogc:And>
                        <ogc:PropertyIsGreaterThan matchCase="true">
                            <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                            <ogc:Literal>{from_id_str}</ogc:Literal>
                        </ogc:PropertyIsGreaterThan>
                        <ogc:PropertyIsLessThan matchCase="true">
                            <ogc:PropertyName>wobj_obj_id</ogc:PropertyName>
                            <ogc:Literal>{to_id_str}</ogc:Literal>
                        </ogc:PropertyIsLessThan>
                    </ogc:And>
                </ogc:Filter>
            </wfs:Query>
        </wfs:GetFeature>
        """

        self._rate_limit()
        try:
            resp = s.post(WOZ_WFS_URL, data=xml_request, timeout=REQUEST_TIMEOUT)
            logger.info("WFS scrape ID range %s - %s (status %d)", from_id_str, to_id_str, resp.status_code)
            return resp.text
        except requests.RequestException as e:
            logger.error("WFS request mislukt voor range %s-%s: %s", from_id_str, to_id_str, e)
            return None

    def parse_wfs_response(self, json_string):
        """Parse WFS GeoJSON response en sla op in legacy PropertyModel."""
        if not json_string:
            return 0

        try:
            data = json.loads(json_string)
        except (json.JSONDecodeError, TypeError) as e:
            logger.error("JSON parse fout: %s", e)
            return 0

        features = data.get("features", [])
        if not features:
            return 0

        saved = 0
        for feature in features:
            props = feature.get("properties", {})
            if not props:
                continue

            obj_id = props.get("wobj_obj_id", "")
            if not obj_id:
                continue

            try:
                PropertyModel.create(
                    identificatie=obj_id,
                    house_number=str(props.get("wobj_huisnummer", "") or ""),
                    house_number_ext=str(props.get("wobj_huisletter", "") or ""),
                    postcode=str(props.get("wobj_postcode", "") or ""),
                    plaatsnaam=str(props.get("wobj_woonplaats", "") or ""),
                    street=str(props.get("wobj_straat", "") or ""),
                    bouwjaar=str(props.get("wobj_bag_bouwjaar", "") or ""),
                    gebruiksdoel=str(props.get("wobj_bag_gebruiksdoel", "") or ""),
                    oppervlakte=str(props.get("wobj_oppervlakte", "") or ""),
                    price_2015=str(props.get("wobj_wrd_woz_waarde", "") or ""),
                    price_2016=str(props.get("wobj_huidige_woz_waarde", "") or ""),
                )
                saved += 1
            except Exception:
                pass  # duplicate

        logger.info("WFS: %d/%d panden opgeslagen", saved, len(features))
        return saved


# --- Main commando's ---

def scrape_postcode(postcode, huisnummer=None, delay=RATE_LIMIT_DELAY):
    """Scrape alle WOZ waarden voor een postcode (optioneel met huisnummer)."""
    scraper = WozScraper(delay=delay)
    adressen = scraper.zoek_adressen_postcode(postcode, huisnummer)

    if not adressen:
        logger.warning("Geen adressen gevonden voor postcode %s", postcode)
        return 0

    opgeslagen = 0
    for adres in adressen:
        if scraper.scrape_adres_en_sla_op(adres):
            opgeslagen += 1

    logger.info("Klaar: %d/%d adressen opgeslagen voor postcode %s", opgeslagen, len(adressen), postcode)
    return opgeslagen


def scrape_adres(adres_tekst, delay=RATE_LIMIT_DELAY):
    """Scrape WOZ waarde voor een specifiek adres."""
    scraper = WozScraper(delay=delay)
    adressen = scraper.zoek_adressen_vrij(adres_tekst)

    if not adressen:
        logger.warning("Geen adressen gevonden voor '%s'", adres_tekst)
        return 0

    opgeslagen = 0
    for adres in adressen:
        if scraper.scrape_adres_en_sla_op(adres):
            opgeslagen += 1

    logger.info("Klaar: %d/%d adressen opgeslagen voor '%s'", opgeslagen, len(adressen), adres_tekst)
    return opgeslagen


def scrape_postcodes_bestand(bestandspad, delay=RATE_LIMIT_DELAY):
    """Scrape WOZ waarden voor alle postcodes in een bestand (1 per regel)."""
    try:
        with open(bestandspad, "r") as f:
            postcodes = [line.strip() for line in f if line.strip()]
    except FileNotFoundError:
        logger.error("Bestand niet gevonden: %s", bestandspad)
        return

    logger.info("Bezig met %d postcodes uit %s", len(postcodes), bestandspad)
    totaal = 0
    for i, postcode in enumerate(postcodes):
        logger.info("--- Postcode %d/%d: %s ---", i + 1, len(postcodes), postcode)
        totaal += scrape_postcode(postcode, delay=delay)

    logger.info("Klaar: totaal %d adressen opgeslagen voor %d postcodes", totaal, len(postcodes))


def scrape_bulk_wfs(from_id=0, to_id=100000, step=5000, threads=4, delay=RATE_LIMIT_DELAY):
    """Bulk scrape via legacy WFS endpoint (object ID ranges)."""
    scraper = WozScraper(delay=delay)

    ranges = []
    current = from_id
    while current < to_id:
        end = min(current + step, to_id)
        ranges.append((current, end))
        current = end

    logger.info(
        "WFS bulk scrape: ID %d tot %d in %d stappen met %d threads",
        from_id, to_id, len(ranges), threads,
    )

    totaal = 0
    with ThreadPoolExecutor(max_workers=threads) as executor:
        futures = {}
        for f, t in ranges:
            future = executor.submit(scraper.scrape_wfs_id_range, f, t)
            futures[future] = (f, t)

        for future in as_completed(futures):
            f, t = futures[future]
            try:
                json_string = future.result()
                saved = scraper.parse_wfs_response(json_string)
                totaal += saved
            except Exception as e:
                logger.error("Fout bij range %d-%d: %s", f, t, e)

    logger.info("WFS bulk scrape klaar: %d panden opgeslagen", totaal)


def exporteer_csv(output_pad="woz_export.csv"):
    """Exporteer WOZ data naar CSV bestand."""
    records = WozWaarde.select()
    count = records.count()
    if count == 0:
        logger.warning("Geen data om te exporteren")
        return

    with open(output_pad, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            "nummeraanduiding_id", "woz_object_nummer",
            "straatnaam", "huisnummer", "huisletter", "huisnummer_toevoeging",
            "postcode", "woonplaats",
            "gebruiksdoel", "oppervlakte", "bouwjaar",
            "laatste_peildatum", "laatste_woz_waarde",
            "alle_woz_waarden",
        ])

        for r in records:
            writer.writerow([
                r.nummeraanduiding_id, r.woz_object_nummer,
                r.straatnaam, r.huisnummer, r.huisletter, r.huisnummer_toevoeging,
                r.postcode, r.woonplaats,
                r.gebruiksdoel, r.oppervlakte, r.bouwjaar,
                r.laatste_peildatum, r.laatste_woz_waarde,
                r.woz_waarden_json,
            ])

    logger.info("Geexporteerd: %d records naar %s", count, output_pad)


def main():
    parser = argparse.ArgumentParser(
        description="WOZ Waardeloket Scraper - Haal WOZ waarden op van Nederlandse panden",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Voorbeelden:
  python run.py --postcode 1017AB
  python run.py --postcode "1017 AB" --huisnummer 10
  python run.py --adres "Dam 1 Amsterdam"
  python run.py --postcodes-file postcodes.txt
  python run.py --bulk --from-id 0 --to-id 100000 --threads 4
  python run.py --export woz_export.csv
        """,
    )

    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--postcode", help="Zoek op postcode (bijv. 1017AB)")
    group.add_argument("--adres", help="Zoek op adres tekst (bijv. 'Dam 1 Amsterdam')")
    group.add_argument("--postcodes-file", help="Bestand met postcodes (1 per regel)")
    group.add_argument("--bulk", action="store_true", help="Bulk scrape via WFS (legacy methode)")
    group.add_argument("--export", nargs="?", const="woz_export.csv", help="Exporteer naar CSV")

    parser.add_argument("--huisnummer", help="Filter op huisnummer (bij --postcode)")
    parser.add_argument("--from-id", type=int, default=0, help="Start object ID (bij --bulk)")
    parser.add_argument("--to-id", type=int, default=100000, help="Eind object ID (bij --bulk)")
    parser.add_argument("--step", type=int, default=5000, help="Stap grootte (bij --bulk)")
    parser.add_argument("--threads", type=int, default=4, help="Aantal threads (bij --bulk)")
    parser.add_argument("--delay", type=float, default=RATE_LIMIT_DELAY, help="Vertraging tussen requests in seconden")

    args = parser.parse_args()

    init_database()

    if args.postcode:
        scrape_postcode(args.postcode, huisnummer=args.huisnummer, delay=args.delay)
    elif args.adres:
        scrape_adres(args.adres, delay=args.delay)
    elif args.postcodes_file:
        scrape_postcodes_bestand(args.postcodes_file, delay=args.delay)
    elif args.bulk:
        scrape_bulk_wfs(
            from_id=args.from_id,
            to_id=args.to_id,
            step=args.step,
            threads=args.threads,
            delay=args.delay,
        )
    elif args.export:
        exporteer_csv(args.export)


if __name__ == "__main__":
    main()
