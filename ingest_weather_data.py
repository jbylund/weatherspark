"""First shuffle the data to s3 then ingest from s3."""
# field definitions found at https://www.ncei.noaa.gov/data/global-hourly/doc/CSV_HELP.pdf
import csv
import datetime
import tempfile

import boto3
import psycopg2
import psycopg2.extras
import requests


def generate_years():
    for iyear in range(datetime.date.today().year - 10, datetime.date.today().year + 1):
        yield iyear


def generate_urls():
    for iyear in generate_years():
        response = requests.get("https://www.ncei.noaa.gov/data/global-hourly/access/{}/".format(iyear))
        response.raise_for_status()
        csvs = [x for x in response.text.split('"') if x.endswith(".csv")]
        for icsv in csvs:
            full_url = "https://www.ncei.noaa.gov/data/global-hourly/access/{iyear}/{icsv}".format(**locals())
            yield full_url


def main():
    with psycopg2.connect(
        user="weather",
        password="xxx",
        database="weather",
    ) as conn:
        conn.cursor_factory = psycopg2.extras.RealDictCursor
        cursor = conn.cursor()
        cursor.execute("""SELECT url FROM ingested_csvs""")
        ingested = set(row["url"] for row in cursor)

    created = False
    for iurl in generate_urls():
        if iurl in ingested:
            continue
        # print(iurl)
        with psycopg2.connect(
            user="weather",
            password="xxx",
            database="weather",
        ) as conn:
            conn.cursor_factory = psycopg2.extras.RealDictCursor
            cursor = conn.cursor()

            if created is False:
                cursor.execute("""CREATE TABLE IF NOT EXISTS ingested_csvs (url text)""")
                cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS unique_url ON ingested_csvs (url)""")
            cursor.execute("""SELECT 1 FROM ingested_csvs WHERE url = %(url)s""", {"url": iurl})
            matches = cursor.fetchall()
            if matches:
                continue
            cursor.execute("""INSERT INTO ingested_csvs (url) VALUES (%(url)s)""", {"url": iurl})

            response = requests.get(iurl)
            response.raise_for_status()

            with tempfile.NamedTemporaryFile(mode="w+") as tfh:
                tfh.write(response.text)
                tfh.flush()
                tfh.seek(0)
                reader = csv.DictReader(tfh)
                cols_present = [x.lower() for x in reader.fieldnames]
                all_cols = cols_present + [
                    x
                    for x in [
                        "ab1",
                        "ac1",
                        "ad1",
                        "ag1",
                        "al1",
                        "al2",
                        "at1",
                        "at2",
                        "at3",
                        "at4",
                        "at5",
                        "at6",
                        "au1",
                        "au2",
                        "aw3",
                        "ay1",
                        "ay2",
                        "az1",
                        "ga4",
                        "ga5",
                        "ga6",
                        "gg1",
                        "gg2",
                        "gg3",
                        "gj1",
                        "hl1",
                        "ia1",
                        "ia2",
                        "me1",
                        "mv1",
                        "mw4",
                        "od2",
                        "od3",
                        "sa1",
                        "ua1",
                        "ug1",
                        "ug2",
                        "ae1",
                        "wa1",
                    ]
                    if x not in cols_present
                ]
                tfh.seek(0)
                fieldspec = "({})".format(", ".join("{} TEXT".format(x) for x in all_cols))
                if created is False:
                    cursor.execute("""CREATE TABLE IF NOT EXISTS weather {}""".format(fieldspec))
                    created = True
                cursor.execute("SELECT * FROM weather WHERE FALSE")
                table_cols = set(col.name for col in cursor.description)
                extra_cols = set(all_cols) - set(table_cols)
                for icol in extra_cols:
                    query = """ALTER TABLE WEATHER ADD COLUMN {} TEXT""".format(icol)
                    print(query)
                    cursor.execute(query)
                try:
                    cursor.copy_expert(
                        "COPY weather ({}) FROM STDIN WITH CSV HEADER".format(", ".join(cols_present)),
                        tfh,
                    )
                    conn.commit()
                except psycopg2.errors.CharacterNotInRepertoire as oops:
                    print(oops, iurl)
                    pass
                except Exception as oops:
                    raise


if "__main__" == __name__:
    main()
