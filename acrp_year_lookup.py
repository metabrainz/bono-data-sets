#!/usr/bin/env python3

import psycopg2
import psycopg2.extras
import re
from datasethoster import Query
from datasethoster.main import app, register_query
from unidecode import unidecode
import config

class ArtistCreditRecordingPairsYearLookupQuery(Query):

    def names(self):
        return ("acrp-year-lookup", "MusicBrainz Artist Credit Recording Pairs Year lookup")

    def inputs(self):
        return ['[artist_credit_name]', '[recording_name]']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a match in MusicBrainz."""

    def outputs(self):
        return ['artist_credit_name', 'recording_name', 'year']

    def fetch(self, params, offset=-1, count=-1):
        artists = []
        recordings = []
        for param in params:
            recording = unidecode(re.sub(r'\W+', '', param['[recording_name]'].lower()))
            artist = unidecode(re.sub(r'\W+', '', param['[artist_credit_name]'].lower()))
            artists.append(artist)
            recordings.append(recording)

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

                index = {}
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    index[row['artist_credit_name'] + row['recording_name']] = row['year']

                results = []
                for param in params:
                    recording = unidecode(re.sub(r'\W+', '', param['[recording_name]'].lower()))
                    artist = unidecode(re.sub(r'\W+', '', param['[artist_credit_name]'].lower()))
                    try:
                        results.append({ 
                                         'artist_credit_name': param['[artist_credit_name]'], 
                                         'recording_name': param['[recording_name]'], 
                                         'year': index[artist+recording] 
                                       })
                    except KeyError:
                        pass

                return results
