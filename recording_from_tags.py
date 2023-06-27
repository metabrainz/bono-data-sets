import psycopg2
import psycopg2.extras
from werkzeug.exceptions import BadRequest
from datasethoster import Query
import config

class RecordingFromTagsQuery(Query):

    def names(self):
        return ("recording-from-tag", "Return random recordings that match given tags")

    def inputs(self):
        return ['[tag]', 'operator', 'threshold']

    def introduction(self):
        return """<p>Return random recordings that have been tagged with one or more tag tags.</p>
                  <p>operator must be either "and" or "or", the logical operation if more than one tag is specified.</p>
                  <p>threshold is the number of times a recording must be tagged with the specified tag for it to be included.</p>"""

    def outputs(self):
        return ['recording_mbid']

    def fetch(self, params, offset=-1, count=100):

        tag = tuple([ p['[tag]'].lower() for p in params ])
        operator = params[0]['operator'].lower()
        if operator not in ("and", "or"):
            raise BadRequest('Operator must be "and" or "or"')

        try:
            count_threshold = int(params[0]['threshold'])
        except ValueError:
            raise BadRequest("threshold must be an integer value.")
        if count_threshold < 1 or count_threshold > 1000:
            raise BadRequest("threshold must be between 1 and 1000.")

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = f"""WITH recordings AS (
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
                                      AND count >= %s
                                 GROUP BY recording_mbid, count, t.name
                              )
                                   SELECT recording_mbid
                                     FROM recordings recs
                                {"WHERE recs.rnum = %d" % len(tag) if operator == "and" else ""}
                                 ORDER BY recs.rand
                                    LIMIT %s"""
                try:
                    curs.execute(query, (tag, count_threshold, count))
                except psycopg2.errors.InvalidTextRepresentation:
                    return []

                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                if len(output) >= count:
                    return output

                rg_query = f"""WITH tags AS (               
                                     SELECT rg.gid AS release_group_mbid
                                          , t.name AS tag_name
                                          , row_number() OVER (PARTITION BY rg.gid ORDER BY t.name) AS rnum
                                       FROM release_group_tag rgt
                                       JOIN release_group rg
                                         ON rgt.release_group = rg.id
                                       JOIN tag t
                                         ON t.id = rgt.tag
                                      WHERE t.name in %s
                                        AND count >= %s 
                                   GROUP BY rg.gid, count, t.name
                                ), release_groups AS (
                                     SELECT release_group_mbid
                                       FROM tags t
                                    {"WHERE recs.rnum = %d" % len(tag) if operator == "and" else ""}
                                ), canonical_release_groups AS (
                                     SELECT DISTINCT canonical_release_mbid
                                       FROM mapping.canonical_release_redirect crr
                                       JOIN release_groups rg
                                         ON rg.release_group_mbid = crr.release_group_mbid
                                ) 
                                     SELECT rec.gid AS recording_mbid
                                          , random() AS rand
                                       FROM canonical_release_groups crg
                                       JOIN release rel
                                         ON rel.gid = crg.canonical_release_mbid                         
                                       JOIN medium m
                                         ON m.release = rel.id                
                                       JOIN track t
                                         ON t.medium = m.id
                                       JOIN recording rec
                                         ON t.recording = rec.id
                                   ORDER BY rand     
                                      LIMIT %s"""
                try:
                    curs.execute(query, (tag, count_threshold, count))
                except psycopg2.errors.InvalidTextRepresentation:
                    return []

                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

                return output
