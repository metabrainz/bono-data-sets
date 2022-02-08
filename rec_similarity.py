from operator import itemgetter

import psycopg2
import psycopg2.extras
from werkzeug.exceptions import BadRequest
from datasethoster import Query
import config


class RecordingSimilarityQuery(Query):

    def names(self):
        return ("recording-similarity", "MusicBrainz Recording Similarity")

    def inputs(self):
        return ['window_size', 'filter_same_artist', 'recording_mbid']

    def introduction(self):
        return """Given a recording MBID, find similar recordings. window_size must be 1, 2, 3, 5 or 10. filter_same_artist should be one of (1, 0, t, f)"""

    def outputs(self):
        return ['similarity', 'artist_credit_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=0, count=50):

        filter_artists = params[0]['filter_same_artist'].lower() in ("1", "true", "t")
        rec_mbid = params[0]['recording_mbid']

        try:
            window_size = int(params[0]['window_size'])
        except ValueError:
            raise BadRequest("window_size must be 1, 2, 3, 5 or 10.")

        if window_size not in (1, 2, 3, 5, 10):
            raise BadRequest("window_size must be 1, 2, 3, 5 or 10.")

        table = "mapping.recording_similarity_index_%d" % window_size

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT canonical_recording_mbid::TEXT
                                  FROM mapping.canonical_recording
                                 WHERE recording_mbid = %s""", (rec_mbid,))
                if curs.rowcount > 0:
                    rec_mbid = curs.fetchone()["canonical_recording_mbid"]
        
                curs.execute("""SELECT acn.artist_credit AS artist_credit_id
                                     , a.gid::TEXT as artist_mbid
                                  FROM recording r
                                  JOIN artist_credit ac
                                    ON r.artist_credit = ac.id
                                  JOIN artist_credit_name acn
                                    ON acn.artist_credit = ac.id
                                  JOIN artist a
                                    ON acn.artist = a.id
                                 WHERE r.gid = %s""", (rec_mbid,))
                if curs.rowcount < 1:
                    raise BadRequest("Artist MBID not found")

                #artist_mbids = [ r["artist_mbid"] for r in curs.fetchall() ]
                artist_credit = curs.fetchone()["artist_credit_id"]

                foo = """   WITH sim AS (
                                    SELECT similarity
                                         , rs.mbid0::TEXT AS recording_mbid_0
                                         , ac0.name AS artist_credit_name_0
                                         , r0.name AS recording_name_0
                                         , r0.artist_credit as artist_credit_0
                                         , rs.mbid1::TEXT AS recording_mbid_1
                                         , ac1.name AS artist_credit_name_1
                                         , r1.name AS recording_name_1
                                         , r1.artist_credit as artist_credit_1
                                      FROM mapping.recording_similarity_index_1 rs
                                      JOIN recording r0
                                        ON r0.gid = rs.mbid0
                                      JOIN artist_credit ac0
                                        ON r0.artist_credit = ac0.id
                                      JOIN recording r1
                                        ON r1.gid = rs.mbid1
                                      JOIN artist_credit ac1
                                        ON r1.artist_credit = ac1.id
                                     WHERE (rs.mbid0 = 'e97f805a-ab48-4c52-855e-07049142113d' OR rs.mbid1 = 'e97f805a-ab48-4c52-855e-07049142113d')
                                  ORDER BY similarity DESC
                                     LIMIT 10 
                                    OFFSET 0
                                ), artists_0 AS (
                                    SELECT acn.artist_credit AS artist_credit_0
                                         , array_agg(a.gid::TEXT) as artist_mbids_0
                                      FROM sim
                                      JOIN artist_credit_name acn
                                        ON sim.artist_credit_1 = acn.artist_credit
                                      JOIN artist a
                                        ON acn.artist = a.id
                                     WHERE acn.artist_credit IN (sim.artist_credit_0) 
                                  GROUP BY acn.artist_credit
                                ), artists_1 AS (
                                    SELECT acn.artist_credit AS artist_credit_1
                                         , array_agg(a.gid::TEXT) as artist_mbids_1
                                      FROM sim
                                      JOIN artist_credit_name acn
                                        ON sim.artist_credit_0 = acn.artist_credit
                                      JOIN artist a
                                        ON acn.artist = a.id
                                     WHERE acn.artist_credit IN (sim.artist_credit_1)
                                  GROUP BY acn.artist_credit
                                )
                            SELECT similarity
                                 , recording_mbid_0
                                 , recording_name_0
                                 , a0.artist_mbids_0
                                 , recording_mbid_1
                                 , recording_name_1
                                 , a1.artist_mbids_1
                              FROM sim
                              JOIN artists_0 a0
                                ON sim.artist_credit_0 = a0.artist_credit_0
                              JOIN artists_1 a1
                                ON sim.artist_credit_1 = a1.artist_credit_1
                                     ;
                """

                curs.execute(f"""SELECT similarity
                                      , rs.mbid0::TEXT AS recording_mbid_0
                                      , ac0.name AS artist_credit_name_0
                                      , ac0.id AS artist_credit_id_0
                                      , r0.name AS recording_name_0
                                      , rs.mbid1::TEXT AS recording_mbid_1
                                      , ac1.name AS artist_credit_name_1
                                      , ac1.id AS artist_credit_id_1
                                      , r1.name AS recording_name_1
                                   FROM {table} rs
                                   JOIN recording r0
                                     ON r0.gid = rs.mbid0
                                   JOIN artist_credit ac0
                                     ON r0.artist_credit = ac0.id
                                   JOIN recording r1
                                     ON r1.gid = rs.mbid1
                                   JOIN artist_credit ac1
                                     ON r1.artist_credit = ac1.id
                                  WHERE (rs.mbid0 = %s OR rs.mbid1 = %s)
                                  LIMIT %s
                                 OFFSET %s""", (rec_mbid, rec_mbid, count, offset))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if row['recording_mbid_0'] == rec_mbid:
                        if filter_artists and row["artist_credit_id_1"] == artist_credit:
                            continue

#                        if filter_artists and any(x in artist_mbids for x in row["artist_mbids_1"]):
#                            continue

                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_credit_name' : row['artist_credit_name_1'], 
                            'recording_name' : row['recording_name_1'],
                            'recording_mbid' : row['recording_mbid_1']
                        })
                    else:
                        if filter_artists and row["artist_credit_id_0"] == artist_credit:
                            continue
#                        if filter_artists and any(x in artist_mbids for x in row["artist_mbids_0"]):
#                            continue

                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_credit_name' : row['artist_credit_name_0'], 
                            'recording_name' : row['recording_name_0'],
                            'recording_mbid' : row['recording_mbid_0']
                        })

                return sorted(relations, key=lambda r: r['similarity'], reverse=True) 
