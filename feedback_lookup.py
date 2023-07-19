import psycopg2
import psycopg2.extras
from datasethoster import Query
import config


class FeedbackLookupQuery(Query):
    '''
        Find top rated loved/hated recordings.
    '''

    def names(self):
        return ("feedback-lookup", "ListenBrainz Recording Feedback")

    def inputs(self):
        return ['score']

    def introduction(self):
        return """Fetch top liked/hated feedback for LB recordings. score must be 1 (loved) or -1 (hated)."""

    def outputs(self):
        return ['recording_mbid', 'count']

    def fetch(self, params, offset=0, count=100):

        score = params[0]["score"]
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT count(recording_mbid) as count
                                , recording_mbid
                             FROM mapping.recording_feedback
                            WHERE score = %s
                         GROUP BY recording_mbid
                         ORDER BY count DESC
                            LIMIT %s
                           OFFSET %s"""
                             
                curs.execute(query, (score, count, offset))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
