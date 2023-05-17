import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class ArtistLookupQuery(Query):

    def names(self):
        return ("artist-lookup", "Lookup and artist name and data from MBID")

    def inputs(self):
        return ['[artist_mbid]']

    def introduction(self):
        return """"""

    def outputs(self):
        return ['artist_mbid', 'artist_name', 'artist_sortname', 'type', 'gender']

    def fetch(self, params, offset=-1, count=-1):

        artist_mbids = tuple([ p['[artist_mbid]'].lower() for p in params ])
        print(artist_mbids)
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT gid AS artist_mbid
                                , name AS artist_name
                                , sortname AS artist_sortname
                                , type
                                , gender
                             FROM artist
                            WHERE gid in %s"""

                curs.execute(query, artist_mbids)
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                return output
