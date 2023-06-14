import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class PopularRecordingsFromTotalListenersQuery(Query):

    def names(self):
        return ("popular-recordings-by-listeners", "Music Listening History Dataset Plus Popular Recordings by total listeners")

    def inputs(self):
        return ['no input required - just type anything here.']

    def introduction(self):
        return """Popular recordings by total listeners derived from the Music Listening History Dataset Plus"""

    def outputs(self):
        return ['recording_name', 'artist_credit_name', 'recording_name', 'recording_mbid', 'total_listeners']

    def fetch(self, params, offset=-1, count=-1):

        if offset < 0:
            offset = 0

        if count < 0:
            count = 100

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT r.name AS recording_name
                                     , r.gid AS recording_mbid
                                     , ac.name AS artist_credit_Name
                                     , total_user_count AS total_listeners
                                  FROM popularity.recording pr
                                  JOIN recording r                                                                                                                   
                                    ON recording_mbid = r.gid 
                                  JOIN artist_credit ac 
                                    ON r.artist_credit = ac.id 
                              ORDER BY total_user_count DESC
                                 LIMIT %s
                                OFFSET %s""", (count, offset))
                return [ dict(row) for row in curs.fetchall() ]
