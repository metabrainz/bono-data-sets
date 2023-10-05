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
        return ['release_group_mbid', 'releases']

    def fetch(self, params, offset=-1, count=-1):

        mbids = [p['[recording_mbid]'] for p in params]
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""WITH release_group_mbids AS MATERIALIZED (
                                         SELECT distinct(rg.id) AS release_group_id
                                              , rg.gid AS release_group_mbid
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
                                         SELECT rgid.release_group_mbid
                                              , rl.gid AS release_mbid
                                              , array_agg(jsonb_build_array(r.gid,            
                                                                             r.name,           
                                                                             r.length,   
                                                                             t.position, 
                                                                             m.position)
                                                          ORDER BY m.position, t.position) AS release         
                                           FROM recording r
                                           JOIN track t
                                             ON t.recording = r.id
                                           JOIN medium m
                                             ON t.medium = m.id
                                           JOIN release rl
                                             ON m.release = rl.id
                                           JOIN release_group_mbids rgid
                                             ON rl.release_group = rgid.release_group_id
                                       GROUP BY rgid.release_group_mbid  
                                              , rl.gid
                                 )
                                         SELECT rr.release_group_mbid::TEXT
                                              , array_agg(jsonb_build_array(rr.release_mbid, rr.release)) AS releases
                                           FROM release_recordings rr
                                       GROUP BY rr.release_group_mbid
                                       """, (tuple(mbids),))

                results = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    row = dict(row)
                    releases = {}
                    for r in row["releases"]:
                        releases[r[0]] = r[1]

                    results.append({ "release_group_mbid": row["release_group_mbid"], "releases": releases })

                return results
