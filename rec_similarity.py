from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
import config


class RecordingSimilarityQuery(Query):

    def names(self):
        return ("recording-similarity", "MusicBrainz Recording Similarity")

    def inputs(self):
        return ['recording_mbid']

    def introduction(self):
        return """Given a recording MBID, find similar recordings."""

    def outputs(self):
        return ['similarity', 'artist_credit_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=0, count=50):

        rec_mbid = params[0]['recording_mbid']
        print(rec_mbid)
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT similarity
                                     , rs.mbid_0::TEXT AS recording_mbid_0
                                     , ac0.name AS artist_credit_name_0
                                     , r0.name AS recording_name_0
                                     , rs.mbid_1::TEXT AS recording_mbid_1
                                     , ac1.name AS artist_credit_name_1
                                     , r1.name AS recording_name_1
                                  FROM mapping.recording_similarity rs
                                  JOIN recording r0
                                    ON r0.gid = rs.mbid_0
                                  JOIN artist_credit ac0
                                    ON r0.artist_credit = ac0.id
                                  JOIN recording r1
                                    ON r1.gid = rs.mbid_1
                                  JOIN artist_credit ac1
                                    ON r1.artist_credit = ac1.id
                                 WHERE (rs.mbid_0 = %s OR rs.mbid_1 = %s)
                                 ORDER BY similarity DESC
                                 LIMIT %s
                                OFFSET %s""", (rec_mbid, rec_mbid, count, offset))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if str(row['recording_mbid_0']) == rec_mbid:
                        relations.append(dict({
                            'similarity' : row['similarity'],
                            'artist_credit_name' : row['artist_credit_name_1'], 
                            'recording_name' : row['recording_name_1'],
                            'recording_mbid' : row['recording_mbid_1']
                        }))
                    else:
                        relations.append(dict({
                            'similarity' : row['similarity'],
                            'artist_credit_name' : row['artist_credit_name_0'], 
                            'recording_name' : row['recording_name_0'],
                            'recording_mbid' : row['recording_mbid_0']
                        }))

                return sorted(relations, key=lambda r: r['similarity'], reverse=True) 
