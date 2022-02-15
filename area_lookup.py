import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class AreaLookupQuery(Query):

    def names(self):
        return ("area-lookup", "Lookup an area given its name.")

    def inputs(self):
        return ['[area]']

    def introduction(self):
        return """Lookup an area code. Must be spelled just like on MusicBrainz."""

    def outputs(self):
        return ['area_mbid', 'area_name']

    def fetch(self, params, offset=-1, count=-1):

        areas = tuple([ p['[area]'].lower() for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT area.gid AS area_mbid,
                                  area.name AS area_name
                             FROM area
                            WHERE lower(area.name) in %s"""


                args = [areas]
                if count > 0:
                    query += " LIMIT %s"
                    args.append(count)
                if offset >= 0:
                    query += " OFFSET %s"
                    args.append(offset)

                curs.execute(query, tuple(args))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    acs.append(dict(row))

                return acs
