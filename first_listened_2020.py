import sys
import uuid

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

psycopg2.extras.register_uuid()

class FirstListenedIn2020Query(Query):
    '''
        Look up a musicbrainz data for a list of recordings, based on MBID. 
    '''

    def names(self):
        return ("first-listened-to-2020", "Tracks first listened to during dumpster fire of a year.")

    def inputs(self):
        return ['user_name']

    def introduction(self):
        return """Look up the tracks a user first listened to in 2020."""

    def outputs(self):
        return ['recording_msid', 'recording_mbid', 'recording_name', 'artist_credit_id', 
                'artist_credit_name', 'listen_count', 'first_listened_at']

    def fetch(self, params, offset=-1, count=-1):

        user_name = tuple([ psycopg2.extensions.adapt(p['user_name']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = '''SELECT m.recording_name, recording_mbid, 
                                  artist_credit_id,
                                  m.artist_credit_name,
                                  listen_count, first_listened_at,
                                  m.recording_msid
                             FROM mapping.first_listened_2020 fl
                             JOIN mapping.first_listened_2020_mapping m
                               ON fl.recording_msid = m.recording_msid
                            WHERE user_name = %s 
                         ORDER BY listen_count DESC'''

                curs.execute(query, tuple(user_name))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
