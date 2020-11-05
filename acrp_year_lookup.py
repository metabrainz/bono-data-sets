#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

class ArtistCreditRecordingPairsYearLookupQuery(Query):

    def names(self):
        return ("acrp-year-lookup", "MusicBrainz Artist Credit Recording Pairs Year lookup")

    def inputs(self):
        return ['artist_credit_name', 'recording_name']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a match in MusicBrainz."""

    def outputs(self):
        return ['artist_credit_name', 'recording_name', 'year']

    def fetch(self, params, offset=-1, count=-1):
        artists = []
        recordings = []
        for param in params:
            artists.append("".join(param['artist_credit_name'].lower().split()))
            recordings.append("".join(param['recording_name'].lower().split()))
        artists = tuple(artists)
        recordings = tuple(recordings)

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT artist_credit_name, 
                                       recording_name,
                                       year
                                  FROM mapping.recording_artist_credit_pairs_year
                                 WHERE artist_credit_name IN %s
                                   AND recording_name IN %s""", (artists, recordings))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
