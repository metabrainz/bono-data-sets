import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class PopularArtistsFromTotalListenersQuery(Query):

    def names(self):
        return ("popular-artists-by-listeners", "Music Listening History Dataset Plus Popular Artists by total listeners")

    def inputs(self):
        return ['no input required - just type anything here.']

    def introduction(self):
        return """Popular artists by total listeners derived from the Music Listening History Dataset Plus"""

    def outputs(self):
        return ['artist_name', 'artist_mbid', 'total_listeners']

    def fetch(self, params, offset=-1, count=-1):

        if offset < 0:
            offset = 0

        if count < 0:
            count = 100

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT a.name AS artist_name
                                     , a.gid AS artist_mbid
                                     , total_user_count AS total_listeners
                                  FROM popularity.artist pa
                                  JOIN artist a
                                    ON artist_mbid = a.gid 
                              ORDER BY total_user_count DESC
                                 LIMIT %s
                                OFFSET %s""", (count, offset))
                return [ dict(row) for row in curs.fetchall() ]
