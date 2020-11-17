import sys
import uuid

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

psycopg2.extras.register_uuid()

class GenreLookupQuery(Query):
    '''
        Look up musicbrainz genre data for a list of recordings, based on MBID. 
    '''

    MAX_ITEMS_PER_RECORDING = 20

    def names(self):
        return ("genre-mbid-lookup", "MusicBrainz Genre/Tag by Recording MBID Lookup")

    def inputs(self):
        return ['recording_mbid']

    def introduction(self):
        return """Look up genres/tags given a recording MBID"""

    def outputs(self):
        return ['recording_mbid', 'genres', 'tags']

    def fetch(self, params, offset=-1, count=-1):

        mbids = tuple([ psycopg2.extensions.adapt(p['recording_mbid']) for p in params ])
        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = '''SELECT r.gid::TEXT AS recording_mbid,
                                  array_agg(t.name) AS tags,
                                  array_agg(g.name) AS genres
                             FROM recording_tag rt
                             JOIN recording r
                               ON r.id = rt.recording
                             JOIN tag t
                               ON rt.tag = t.id
                        LEFT JOIN genre g
                               ON g.name = t.name
                            WHERE r.gid 
                               IN %s
                         GROUP BY r.gid
                         ORDER BY r.gid'''

                args = [mbids]
                if count > 0:
                    query += " LIMIT %s"
                    args.append(count)
                if offset >= 0:
                    query += " OFFSET %s"
                    args.append(offset)

                curs.execute(query, tuple(args))
                index = {}
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    data = dict(row)
                    data['tags'] = ",".join([ d for d in data['tags'][:self.MAX_ITEMS_PER_RECORDING] if d])
                    data['genres'] = ",".join([ d for d in data['genres'][:self.MAX_ITEMS_PER_RECORDING] if d])
                    
                    index[row['recording_mbid']] = data

                output = []
                for p in params:
                    mbid = p['recording_mbid']
                    try:
                        output.append({ 'recording_mbid': mbid, 'tags': index[mbid]['tags'], 'genres': index[mbid]['genres'] })
                    except KeyError:
                        output.append({ 'recording_mbid': mbid, 'tags': '', 'genres': '' })

        return output
