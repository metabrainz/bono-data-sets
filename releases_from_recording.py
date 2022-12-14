import psycopg2
import psycopg2.extras
from datasethoster import Query
import config


class ReleasesFromRecordingQuery(Query):

    def names(self):
        return ("releases-from-recordings", "Lookup which releases contain a given recording")

    def inputs(self):
        return ['[recording_mbid]']

    def introduction(self):
        return """Look the release MBIDs given a recording MBID"""

    def outputs(self):
        return ['src_recording_mbid', 'release_group_mbid', 'release_name', 'release_mbid', 'release']

    def fetch(self, params, offset=-1, count=-1):

        mbids = [p['[recording_mbid]'] for p in params]
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""WITH release_mbids AS MATERIALIZED (
                                         SELECT rl.gid AS release_mbid
                                              , rg.gid AS release_group_mbid
                                              , r.gid AS recording_mbid
                                              , rl.name AS release_name
                                           FROM release rl
                                           JOIN medium m
                                             ON m.release = rl.id
                                           JOIN track t
                                             ON t.medium = m.id
                                           JOIN recording r
                                             ON t.recording = r.id
                                           JOIN release_group rg
                                             ON rl.release_group = rg.id
                                          WHERE r.gid IN %s
                                ), release_recordings AS (
                                         SELECT rid.recording_mbid AS src_recording_mbid 
                                              , rid.release_mbid AS release_mbid
                                              , rid.release_group_mbid AS release_group_mbid
                                              , rid.release_name AS release_name
                                              , r.gid AS recording_mbid
                                              , r.name AS recording_name
                                              , r.length AS duration
                                              , t.position
                                              , m.position as medium_position
                                           FROM recording r
                                           JOIN track t
                                             ON t.recording = r.id
                                           JOIN medium m
                                             ON t.medium = m.id
                                           JOIN release rl
                                             ON m.release = rl.id
                                           JOIN release_mbids rid
                                             ON rl.gid = rid.release_mbid
                                       ORDER BY rid.release_mbid
                                              , m.position
                                              , t.position
                                )
                                         SELECT src_recording_mbid
                                              , rr.release_mbid
                                              , rr.release_group_mbid
                                              , rr.release_name
                                              , array_agg(jsonb_build_object('recording_mbid', rr.recording_mbid,
                                                                             'recording_name', rr.recording_name,
                                                                             'duration', rr.duration,
                                                                             'position', rr.position,
                                                                             'medium_position', rr.medium_position)
                                                          ORDER BY rr.medium_position, rr.position) AS release
                                           FROM release_recordings rr
                                       GROUP BY src_recording_mbid
                                              , rr.release_group_mbid
                                              , rr.release_mbid
                                              , rr.release_name""", (tuple(mbids),))

                results = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    results.append(dict(row))

                return results
