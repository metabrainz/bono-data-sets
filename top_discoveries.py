import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class TopDiscoveriesQuery(Query):
    '''
        Fetch a list of top discoveries for the current year
    '''

    def names(self):
        return ("top-discoveries", "Tracks first listened to this year.")

    def inputs(self):
        return ['user_id']

    def introduction(self):
        return """Look up the tracks a user first listened to this year."""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'artist_credit_name', 'artist_mbids', 'listen_count', 'user_id']

    def fetch(self, params, offset=-1, count=-1):

        user_id = tuple([ psycopg2.extensions.adapt(p['user_id']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = '''SELECT * 
                             FROM mapping.top_discoveries
                            WHERE user_id = %s 
                         ORDER BY listen_count DESC'''

                curs.execute(query, tuple(user_id))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
