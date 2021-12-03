import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

psycopg2.extras.register_uuid()

class GenreLookupQuery(Query):
    '''
        Look up musicbrainz genre data for a list of recordings, based on MBID.
    '''

    MAX_ITEMS_PER_RECORDING = 20

    def names(self):
        return ("genre-mbid-lookup", "MusicBrainz Genre/Tag by Recording MBID Lookup")

    def inputs(self):
        return ['[recording_mbid]']

    def introduction(self):
        return """Look up genres/tags given a recording MBID"""

    def outputs(self):
        return ['recording_mbid', 'genres', 'tags']

    def fetch(self, params, offset=-1, count=-1):

        mbids = tuple([ psycopg2.extensions.adapt(p['[recording_mbid]']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT *
                             FROM mapping.recording_tags
                            WHERE recording_mbid in %s"""


                args = [mbids]

                output = []
                curs.execute(query, tuple(args))
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                return output
