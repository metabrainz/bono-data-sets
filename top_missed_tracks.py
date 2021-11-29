import datetime

import psycopg2
import psycopg2.extras
from datasethoster import Query
import requests
import config

psycopg2.extras.register_uuid()

class TopMissedTracksQuery(Query):
    '''
        Fetch a list of top tracks that a user's top similar users listened to, but that user didn't.
    '''

    def names(self):
        return ("top-missed-tracks", "Tracks that a user's most similar users listened to in 2021, but the user themselves didn't.")

    def inputs(self):
        return ['user_name']

    def introduction(self):
        return """This query returns tracks that your most similar users listened to a lot, but that did not
                  appear in your own listening history for 2021."""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'artist_credit_name', 'listen_count']

    def fetch(self, params, offset=0, count=50):

        user_name = params[0]["user_name"]
        r = requests.get(f"https://api.listenbrainz.org/1/user/{user_name}/similar-users")
        if r.status_code != 200:
            return []

        similar_users = []
        for row in r.json()["payload"]:
            similar_users.append(row["user_name"])

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                query = """SELECT q.recording_mbid
                                , r.name AS recording_name
                                , acn.name AS artist_credit_name
                                , sum(listen_count) AS listen_count
                             FROM (
                                   SELECT recording_mbid
                                     FROM mapping.tracks_of_the_year t
                                    WHERE user_name = %s
                                    UNION              
                                   SELECT recording_mbid
                                     FROM mapping.tracks_of_the_year t
                                    WHERE user_name = %s
                                    UNION   
                                   SELECT recording_mbid
                                     FROM mapping.tracks_of_the_year t
                                    WHERE user_name = %s
                                   EXCEPT
                                   SELECT recording_mbid
                                     FROM mapping.tracks_of_the_year t
                                    WHERE user_name = %s
                                  ) AS q
                               JOIN mapping.tracks_of_the_year t
                                 ON t.recording_mbid = q.recording_mbid
                               JOIN recording r
                                 ON q.recording_mbid = r.gid
                               JOIN artist_credit ac
                                 ON r.artist_credit = ac.id
                               JOIN artist_credit_name acn
                                 ON acn.artist_credit = ac.id
                              WHERE user_name IN (%s, %s, %s)
                           GROUP BY q.recording_mbid, r.name, acn.name
                           ORDER BY listen_count DESC
                              LIMIT 50"""

                users = similar_users[:3]
                users.append(user_name)
                users.extend(similar_users[:3])
                print(users)
                curs.execute(query, tuple(users))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
