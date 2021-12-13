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
        return ['recording_mbid', 'recording_name', 'artist_credit_name', 'artist_mbids', 'listen_count']

    def fetch(self, params, offset=0, count=50):

        user_name = params[0]["user_name"]
        r = requests.get(f"https://api.listenbrainz.org/1/user/{user_name}/similar-users")
        if r.status_code != 200:
            return []

        similar_users = []
        for row in r.json()["payload"]:
            similar_users.append(row["user_name"])

        if len(similar_users) == 0:
            return []

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:

                query = """WITH exclude_tracks AS (
                           SELECT recording_mbid
                             FROM mapping.tracks_of_the_year t
                            WHERE user_name = %s
                       ) SELECT recording_mbid
                              , recording_name
                              , artist_credit_name
                              , artist_mbids
                              , sum(listen_count) AS listen_count
                             FROM mapping.tracks_of_the_year t
                            WHERE user_name IN (%s, %s, %s)
                              AND recording_mbid NOT IN (SELECT * FROM exclude_tracks)
                         GROUP BY recording_mbid, recording_name, artist_credit_name
                         ORDER BY listen_count DESC
                            LIMIT 100"""

                users = [ user_name ] 
                users.extend(similar_users[:3])
                curs.execute(query, tuple(users))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
