import datetime

import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class TopNewTracksQuery(Query):
    '''
        Fetch a list of top new tracks for the current year
    '''

    def names(self):
        return ("top-new-tracks", "Tracks released this year a user has listened to this year.")

    def inputs(self):
        return ['user_name']

    def introduction(self):
        return """Look up a users top tracks that were released in the current year."""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'artist_credit_name', 'artist_mbids', 'listen_count']

    def fetch(self, params, offset=0, count=50):

        user_name = tuple([ psycopg2.extensions.adapt(p['user_name']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                query = """SELECT t.recording_mbid
                                , t.artist_credit_name
                                , t.artist_mbids
                                , t.recording_name
                                , listen_count
                             FROM mapping.tracks_of_the_year t 
                             JOIN mapping.year_mapping y 
                               ON t.recording_mbid = y.recording_mbid 
                            WHERE year = %s
                              AND user_name = %s
                         ORDER BY listen_count DESC
                           OFFSET %s 
                            LIMIT %s"""

                curs.execute(query, (datetime.datetime.now().year, user_name, offset, count))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
