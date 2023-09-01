from operator import itemgetter

import psycopg2
import psycopg2.extras
from werkzeug.exceptions import BadRequest
from datasethoster import Query
import config
from popular_tags import POPULAR_TAGS


class TagSimilarityQuery(Query):

    def names(self):
        return ("tag-similarity", "ListenBrainz Tag Similarity")

    def inputs(self):
        return ['tag']

    def introduction(self):
        return """Given a tag, find similar tags ignoring the most popular tags. (e.g. pop, rock, punk, etc)"""

    def outputs(self):
        return ['tag_0', 'tag_1', 'count']

    def fetch(self, params, offset=0, count=50):

        tag = params[0]['tag']
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                curs.execute("""SELECT id FROM tag WHERE name = %s""", (tag,))
                row = curs.fetchone()
                if not row:
                    return []

                tag_id = row["id"]

                curs.execute(f"""SELECT count
                                      , t0.name AS tag_0
                                      , t1.name AS tag_1
                                   FROM similarity.tag_similarity ts
                                   JOIN tag t0
                                     ON t0.id = ts.tag_0
                                   JOIN tag t1
                                     ON t1.id = ts.tag_1
                                  WHERE (ts.tag_0 = %s OR ts.tag_1 = %s)
                                  LIMIT %s
                                 OFFSET %s""", (tag_id, tag_id, count, offset))
                relations = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    if row['tag_0'] == tag:
                        if row['tag_1'] in POPULAR_TAGS:
                            continuet
                        relations.append({
                            'tag_0' : row['tag_0'], 
                            'tag_1' : row['tag_1'], 
                            'count' : int(row['count']),
                        })
                    else:
                        if row['tag_0'] in POPULAR_TAGS:
                            continue
                        relations.append({
                            'tag_0' : row['tag_0'], 
                            'tag_1' : row['tag_1'], 
                            'count' : int(row['count']),
                        })

                return sorted(relations, key=lambda r: r['count'], reverse=True) 
