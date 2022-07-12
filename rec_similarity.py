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
        return ['table', 'recording_mbid']

    def introduction(self):
        return """Given a recording MBID, find similar recordings. Table must be a recording similarity
                  table in the similarity schema. (steps_5_days_1000_session_1800_threshold_5, 
                  steps_5_days_365_session_1800_threshold_5 or steps_5_days_730_session_1800_threshold_5)"""

    def outputs(self):
        return ['similarity', 'artist_credit_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=0, count=50):

        rec_mbid = params[0]['recording_mbid']
        table = params[0]['table']
        table = f"similarity.{table}"

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT canonical_recording_mbid::TEXT
                                  FROM mapping.canonical_recording_redirect
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
                    print("no artist credit found")
                    raise BadRequest("Artist MBID not found")

                artist_credit = curs.fetchone()["artist_credit_id"]
                print("artist_credit %d" % artist_credit)

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
                                     ON r0.gid::TEXT = rs.mbid0
                                   JOIN artist_credit ac0
                                     ON r0.artist_credit = ac0.id
                                   JOIN recording r1
                                     ON r1.gid::TEXT = rs.mbid1
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
                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_credit_name' : row['artist_credit_name_1'], 
                            'recording_name' : row['recording_name_1'],
                            'recording_mbid' : row['recording_mbid_1']
                        })
                    else:
                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_credit_name' : row['artist_credit_name_0'], 
                            'recording_name' : row['recording_name_0'],
                            'recording_mbid' : row['recording_mbid_0']
                        })

                return sorted(relations, key=lambda r: r['similarity'], reverse=True) 
