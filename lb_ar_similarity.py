from operator import itemgetter

import psycopg2
import psycopg2.extras
from werkzeug.exceptions import BadRequest
from datasethoster import Query
import config


class ArtistSimilarityQuery(Query):

    def names(self):
        return ("artist-similarity", "ListenBrainz Artist Similarity")

    def inputs(self):
        return ['window_size', 'artist_mbid']

    def introduction(self):
        return """Given an artist MBID, find similar artist."""

    def outputs(self):
        return ['similarity', 'artist_name', 'artist_mbid']

    def fetch(self, params, offset=0, count=50):

        ar_mbid = params[0]['artist_mbid']
        try:
            window_size = int(params[0]['window_size'])
        except ValueError:
            raise BadRequest("window_size must be 1, 2, or 3.")

        if window_size not in (1, 2, 3):
            raise BadRequest("window_size must be 1, 2, or 3.")

        table = "mapping.artist_similarity_index_%d" % window_size
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute(f"""SELECT similarity
                                      , ars.mbid0::TEXT AS artist_mbid_0
                                      , a0.name AS artist_name_0
                                      , ars.mbid1::TEXT AS artist_mbid_1
                                      , a1.name AS artist_name_1
                                   FROM {table} ars
                                   JOIN artist a0
                                     ON a0.gid = ars.mbid0
                                   JOIN artist a1
                                     ON a1.gid = ars.mbid1
                                  WHERE (ars.mbid0::TEXT = %s OR ars.mbid1::TEXT = %s)
                                  LIMIT %s
                                 OFFSET %s""", (ar_mbid, ar_mbid, count, offset))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if row['artist_mbid_0'] == ar_mbid:
                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_name' : row['artist_name_1'], 
                            'artist_mbid' : row['artist_mbid_1']
                        })
                    else:
                        relations.append({
                            'similarity' : int(row['similarity']),
                            'artist_name' : row['artist_name_0'], 
                            'artist_mbid' : row['artist_mbid_0']
                        })

                return sorted(relations, key=lambda r: r['similarity'], reverse=True) 
