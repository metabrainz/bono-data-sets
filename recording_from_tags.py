import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

class RecordingFromTagsQuery(Query):

    def names(self):
        return ("recording-from-tag", "Return random recordings that match given tags")

    def inputs(self):
        return ['[tag]']

    def introduction(self):
        return """Return random recordings that have been tagged with one or more tag tags."""

    def outputs(self):
        return ['recording_mbid']

    def fetch(self, params, offset=-1, count=100):

        tag = tuple([ p['[tag]'].lower() for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """WITH recordings AS (
                                  SELECT rec.gid AS recording_mbid
                                       , t.name AS tag_name
                                       , row_number() OVER (PARTITION BY rec.gid ORDER BY t.name) AS rnum
                                       , random() AS rand
                                    FROM recording rec
                                    JOIN recording_tag rt
                                      ON rec.id = rt.recording
                                    JOIN tag t
                                      ON t.id = rt.tag
                                   WHERE t.name in %s
                                GROUP BY recording_mbid, count, t.name
                             )
                                  SELECT recording_mbid
                                    FROM recordings recs
                                   WHERE recs.rnum = %s
                                ORDER BY recs.rand
                                   LIMIT %s"""
#                                     AND count > 1
                try:
                    curs.execute(query, (tag, len(tag), count))
                except psycopg2.errors.InvalidTextRepresentation:
                    return []

                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                return output
