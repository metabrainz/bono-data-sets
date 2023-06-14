import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class PopularRecordingsFromTotalListenCountQuery(Query):

    def names(self):
        return ("popular-recordings-by-listen-count", "Music Listening History Dataset Plus Popular Recordings")

    def inputs(self):
        return ['no input required - just type anything here.']

    def introduction(self):
        return """Popular recordings by total listen counts derived from the Music Listening History Dataset Plus"""

    def outputs(self):
        return ['recording_name', 'artist_credit_name', 'recording_mbid', 'total_listen_count']

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
                                     , total_listen_count
                                  FROM popularity.recording pr
                                  JOIN recording r                                                                                                                   
                                    ON recording_mbid = r.gid 
                                  JOIN artist_credit ac 
                                    ON r.artist_credit = ac.id 
                              ORDER BY total_listen_count DESC
                                 LIMIT %s
                                OFFSET %s""", (count, offset))
                return [ dict(row) for row in curs.fetchall() ]
