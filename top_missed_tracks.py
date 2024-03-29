from time import sleep
import datetime

from werkzeug.exceptions import InternalServerError
import psycopg2
import psycopg2.extras
from datasethoster import Query
import requests
import config

class TopMissedTracksQuery(Query):
    '''
        Fetch a list of top tracks that a user's top similar users listened to, but that user didn't.
    '''

    def names(self):
        return ("top-missed-tracks", "Tracks that a user's most similar users listened to in 2021, but the user themselves didn't.")

    def inputs(self):
        return ['user_id', 'user_name']

    def introduction(self):
        return """This query returns tracks that your most similar users listened to a lot, but that did not
                  appear in your own listening history for 2021."""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'artist_credit_name', 'artist_mbids', 'listen_count']

    def fetch(self, params, offset=0, count=50):

        user_name = params[0]["user_name"]
        user_id = params[0]["user_id"]
        while True:
            r = requests.get(f"https://api.listenbrainz.org/1/user/{user_name}/similar-users")
            if r.status_code == 429:
                print("429! Sleeping 1s")
                sleep(1)
                continue

            if r.status_code == 404:
                return []

            if r.status_code == 200:
                break

            raise InternalServerError("Cannot fetch similar users.")

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
                            WHERE user_id = %s
                       ) SELECT recording_mbid
                              , recording_name
                              , artist_credit_name
                              , artist_mbids
                              , sum(listen_count) AS listen_count
                             FROM mapping.tracks_of_the_year t
                            WHERE user_id IN (%s, %s, %s)
                              AND recording_mbid NOT IN (SELECT * FROM exclude_tracks)
                         GROUP BY recording_mbid, recording_name, artist_credit_name, artist_mbids
                         ORDER BY listen_count DESC
                            LIMIT 100"""

                users = [ user_id ] 
                users.extend(similar_users[:3])
                curs.execute(query, tuple(users))
                output = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    output.append(dict(row))

        return output
