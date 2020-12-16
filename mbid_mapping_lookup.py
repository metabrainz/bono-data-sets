#!/usr/bin/env python3

import re
import ujson

import psycopg2
import psycopg2.extras
import typesense
import typesense.exceptions
from datasethoster import Query
from datasethoster.main import app, register_query
from unidecode import unidecode
from Levenshtein import distance

import config

def prepare_query(text):
    return unidecode(re.sub(" +", " ", re.sub(r'[^\w ]+', '', text)).lower())

class MBIDMappingSearch(Query):

    EDIT_DIST_THRESHOLD = 5

    def __init__(self):
        self.good = 0
        self.good_alt = 0
        self.bad = 0
        self.total = 0
        self.debug = True

        self.client = typesense.Client({
            'nodes': [{
              'host': config.TYPESENSE_HOST,
              'port': '8108',
              'protocol': 'http',
            }],
            'api_key': config.TYPESENSE_API_KEY,
            'connection_timeout_seconds': 2
        })

    def report(self):
        print("%d matched (%d alt matches), %d unmatched. %.f%% good matches" % (self.good, self.good_alt, self.bad, 100.0 * (float(self.good) / self.total)))

    # Other possible detunings:
    #  - Swap artist/recording
    #  - remove track numbers from recording
    def detune_query_string(self, query):

        for s in ("(", "[", " ft ", " ft. ", " feat ", " feat. ", " featuring "):
            index = query.find(s)
            if index >= 0:
                return query[:index].strip()

        return ""

        
    def lookup(self, artist_credit_name_p, recording_name_p):

        search_parameters = {
            'q'         : artist_credit_name_p + " " + recording_name_p,
            'query_by'  : "combined",
            'prefix'    : 'no',
            'num_typos' : 5
        }

        hits = self.client.collections['recording_artist_credit_mapping'].documents.search(search_parameters)
        if len(hits["hits"]) == 0:
            self.total += 1
            self.bad += 1
            return None

        return hits["hits"][0]

    def compare(self, artist_credit_name, recording_name, artist_credit_name_hit, recording_name_hit):
        """
            Compare the fiels, print debug info if turn on, and return edit distance as (a_dist, r_dist)
        """

        if self.debug:
            print("Q %-60s %-60s" % (artist_credit_name[:59], recording_name[:59]))
            print("H %-60s %-60s" % (artist_credit_name_hit[:59], recording_name_hit[:59]))

        return (distance(artist_credit_name, artist_credit_name_hit), distance(recording_name, recording_name_hit))

    def evaluate_hit(self, hit, artist_credit_name, recording_name):
        """
            Evaluate the given prepared search terms and hit. If the hit doesn't match,
            attempt to detune it and try again for detuned artist and detuned recording.
            If the hit is good enough, return it, otherwise return None.
        """
        ac_hit = hit['document']['artist_credit_name']
        r_hit = hit['document']['recording_name']

        ac_detuned = self.detune_query_string(ac_hit)
        r_detuned = self.detune_query_string(r_hit)

        is_alt = False
        while True:
            ac_dist, r_dist = self.compare(artist_credit_name, recording_name, prepare_query(ac_hit), prepare_query(r_hit))
            if ac_dist <= self.EDIT_DIST_THRESHOLD and r_dist <= self.EDIT_DIST_THRESHOLD:
                if is_alt:
                    self.good_alt += 1
                return hit

            if ac_dist > self.EDIT_DIST_THRESHOLD and ac_detuned:
                ac_hit = ac_detuned
                ac_detuned = ""
                is_alt = True
                continue

            if r_dist > self.EDIT_DIST_THRESHOLD and r_detuned:
                r_hit = r_detuned
                r_detuned = ""
                is_alt = True
                continue

            return None


    def search(self, artist_credit_name, recording_name):

        if self.debug:
            print("- %-60s %-60s" % (artist_credit_name[:59], recording_name[:59]))

        artist_credit_name_p = prepare_query(artist_credit_name)
        recording_name_p = prepare_query(recording_name)

        ac_detuned = prepare_query(self.detune_query_string(artist_credit_name_p))
        r_detuned = prepare_query(self.detune_query_string(recording_name_p))

        tries = 0
        while True:
           
            tries += 1
            hit = self.lookup(artist_credit_name_p, recording_name_p)
            if hit:
                hit = self.evaluate_hit(hit, artist_credit_name_p, recording_name_p)

            if not hit:
                hit = None
                if ac_detuned:
                    artist_credit_name_p = ac_detuned
                    ac_detuned = None
                    continue

                if r_detuned:
                    recording_name_p = r_detuned
                    r_detuned = None
                    continue

                if self.debug:
                    print("FAIL.\n");
                break

            break

        self.total += 1
        if not hit:
            self.bad += 1
            return None

        self.good += 1
        if self.debug:
            print("OK.\n");

        return { 'artist_credit_name': hit['document']['artist_credit_name'],
                 'artist_credit_id': hit['document']['artist_credit_id'],
                 'release_name': hit['document']['release_name'],
                 'release_mbid': hit['document']['release_mbid'],
                 'recording_name': hit['document']['recording_name'],
                 'recording_mbid': hit['document']['recording_mbid'] }


    def lookup_rows(self):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT DISTINCT recording_msid, artist_credit_name, recording_name 
                                           FROM first_listened_in_2020_artist_recording
                                       ORDER BY artist_credit_name, recording_name
                                          LIMIT 5000 OFFSET 520700""")
                for row in curs.fetchall():
                    self.search(row['artist_credit_name'], row['recording_name'])

mapping = MBIDMappingSearch()
mapping.lookup_rows()
mapping.report()
