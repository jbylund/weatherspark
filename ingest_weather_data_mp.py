"""First shuffle the data to s3 then ingest from s3."""
# field definitions found at https://www.ncei.noaa.gov/data/global-hourly/doc/CSV_HELP.pdf
import csv
import datetime
import multiprocessing
import tempfile
import time

import boto3
import psycopg2
import psycopg2.extras
import requests


class ImportWorker(multiprocessing.Process):
    def __init__(self, queue):
        super().__init__()
        self.queue = queue
        self.conn = None

    def run(self):
        with psycopg2.connect(
            user="weather",
            password="xxx",
            database="weather",
        ) as self.conn:
            self.conn.cursor_factory = psycopg2.extras.RealDictCursor
            self.cursor = self.conn.cursor()

            self.set_loaded()

            while True:
                iurl = self.queue.get()
                if iurl is None:
                    self.queue.put(iurl)
                    return
                self.load_one_url(iurl)

    def set_loaded(self):
        self.cursor.execute("""SELECT url FROM ingested_csvs""")
        self.conn.commit()
        self.loaded = set(row["url"] for row in self.cursor)

    def already_loaded(self, url):
        if url in self.loaded:
            return True
        self.cursor.execute("""SELECT 1 FROM ingested_csvs WHERE url = %(url)s""", {"url": url})
        return bool(self.cursor.fetchall())

    def load_one_url(self, url):
        if self.already_loaded(url):
            return

        self.cursor.execute("""INSERT INTO ingested_csvs (url) VALUES (%(url)s)""", {"url": url})

        response = requests.get(url)
        response.raise_for_status()

        with tempfile.NamedTemporaryFile(mode="w+") as tfh:
            tfh.write(response.text)
            tfh.flush()
            tfh.seek(0)
            reader = csv.DictReader(tfh)

            def row_to_args(row):
                parsed = {
                    "call_sign": row["CALL_SIGN"],
                    "name": row["NAME"],
                    "station": row["STATION"],
                    "timepoint": row["DATE"],
                }
                if "999" not in row["TMP"]:
                    parsed["temp_c"], _, parsed["temp_q"] = row["TMP"].partition(",")
                if "999" not in row["DEW"]:
                    parsed["dew_c"], _, parsed["dew_q"] = row["DEW"].partition(",")
                for icol in ["temp_c", "dew_c"]:
                    if icol in parsed:
                        parsed[icol] = int(parsed[icol])
                    else:
                        parsed[icol[:-1] + "q"] = None
                        parsed[icol] = None
                return parsed

            insert_query = (
                " ".join(
                    """INSERT INTO weather
            (   call_sign,     dew_c,     dew_q,     name,     station,     temp_c,     temp_q,     timepoint) VALUES""".split()
                )
                + " "
            )

            while True:
                merged_args = {}
                nrows = 0
                try:
                    for row in reader:
                        row_args = row_to_args(row)
                        for key, val in row_args.items():
                            merged_args["{}_{}".format(nrows, key)] = val
                        nrows += 1
                        if 100 < nrows:
                            break
                except:
                    break  # csv was bad
                if not merged_args:
                    break
                args_part_of_query = ", ".join(
                    """( %({idx}_call_sign)s, %({idx}_dew_c)s, %({idx}_dew_q)s, %({idx}_name)s, %({idx}_station)s, %({idx}_temp_c)s, %({idx}_temp_q)s, %({idx}_timepoint)s)""".format(
                        idx=idx
                    )
                    for idx in range(nrows)
                )
                full_query = insert_query + args_part_of_query
                self.cursor.execute(full_query, merged_args)
        self.conn.commit()


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


def get_cols():
    with psycopg2.connect(
        user="weather",
        password="xxx",
        database="weather",
    ) as conn:
        cursor.execute("SELECT * FROM weather WHERE FALSE")
        return set(col.name for col in cursor.description)


def main():
    with psycopg2.connect(
        user="weather",
        password="xxx",
        database="weather",
    ) as conn:
        cursor = conn.cursor()
        cursor.execute("""DROP TABLE IF EXISTS weather""")
        # cursor.execute("""DROP TABLE IF EXISTS ingested_csvs""")

        cursor.execute(
            """CREATE TABLE weather (call_sign text, dew_c int, dew_q text, name text, station text, temp_c int, temp_q text, timepoint text)"""
        )
        cursor.execute("""CREATE TABLE IF NOT EXISTS ingested_csvs (url text)""")
        cursor.execute("""CREATE UNIQUE INDEX IF NOT EXISTS unique_url ON ingested_csvs (url)""")
        conn.commit()
    work_queue = multiprocessing.Queue()
    workers = [ImportWorker(work_queue) for _ in range(multiprocessing.cpu_count())]
    print("Starting workers...")
    for iworker in workers:
        iworker.start()
    print("Queuing work...")
    for iurl in generate_urls():
        work_queue.put(iurl)
    work_queue.put(None)
    while 10 < work_queue.qsize():
        print("There are {:,} jobs queued...".format(work_queue.qsize()))
        time.sleep(5)
    print("Joining workers...")
    for idx, iworker in enumerate(workers):
        iworker.join()
        print("Worker {} done...".format(idx))
    work_queue.get()


if "__main__" == __name__:
    main()
