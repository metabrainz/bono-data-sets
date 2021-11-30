import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

psycopg2.extras.register_uuid()

class BPMKeyLookupQuery(Query):
    '''
        Look up BPM and key for a recording_mbid
    '''

    def names(self):
        return ("bpm-key-lookup", "AcousticBrainz BMP/Key Recording by MBID Lookup")

    def inputs(self):
        return ['[recording_mbid]']

    def introduction(self):
        return """Look up BPM/key given a recording MBID"""

    def outputs(self):
        return ['recording_mbid', 'bpm', 'key']

    def fetch(self, params, offset=-1, count=-1):

        mbids = tuple([ psycopg2.extensions.adapt(p['[recording_mbid]']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT recording_mbid
                                , bpm
                                , ''
                             FROM mapping.recording_bpm_key
                            WHERE recording_mbid in %s"""

                args = [mbids]
                curs.execute(query, tuple(args))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                return output
