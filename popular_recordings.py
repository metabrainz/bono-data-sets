import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class PopularRecordingsByArtistQuery(Query):

    def names(self):
        return ("popular-recordings", "Popular recordings by artist")

    def inputs(self):
        return ['[artist_mbid]']

    def introduction(self):
        return """Fetch popular tracks given an artist_mbid"""

    def outputs(self):
        return ['artist_mbid', 'recording_mbid', 'count']

    def fetch(self, params, offset=-1, count=-1):

        artist_mbids = [ p['[artist_mbid]'].lower() for p in params ]
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT artist_mbid
                                     , recording_mbid
                                     , count
                                  FROM mapping.popular_tracks
                                 WHERE artist_mbid IN %s
                              ORDER BY artist_mbid, count DESC""", (tuple(artist_mbids),))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    acs.append(dict(row))

                return acs
