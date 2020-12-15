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
    #  - remove trailing parens / brackets from recording
    #  - remove track numbers from recording
    def detune_query_string(self, query):

        print("detune: '%s'" % query)
        index = query.find("(")
        if index >= 0:
            return query[:index].strip()

        index = query.find("[")
        if index >= 0:
            return query[:index].strip()

        index = query.find(" ft ")
        if index >= 0:
            return query[:index].strip()

        index = query.find(" feat ")
        if index >= 0:
            return query[:index].strip()

        return None

        
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

    def search(self, artist_credit_name, recording_name):

        if self.debug:
            print("- %-60s %-60s" % (artist_credit_name[:59], recording_name[:59]))

        artist_credit_name_p = prepare_query(artist_credit_name)
        ac_detuned = self.detune_query_string(artist_credit_name_p)

        recording_name_p = prepare_query(recording_name)
        r_detuned = self.detune_query_string(recording_name_p)

        tries = 0
        alt_try = False
        while True:
           
            tries += 1
            hit = self.lookup(artist_credit_name_p, recording_name_p)
            if hit:
                a_dist, r_dist = self.compare(artist_credit_name_p, recording_name_p,
                                              prepare_query(hit['document']['artist_credit_name']),
                                              prepare_query(hit['document']['recording_name']))

            print(a_dist, r_dist)
            if a_dist > self.EDIT_DIST_THRESHOLD:
                detuned = self.detune_query_string(hit['document']['artist_credit_name'])
                if detuned:
                    print("detuned '%s'" % detuned)
                    a_dist, r_dist = self.compare(artist_credit_name_p, recording_name_p,
                                                  prepare_query(detuned),
                                                  prepare_query(hit['document']['recording_name']))

            if not hit or a_dist > self.EDIT_DIST_THRESHOLD or r_dist > self.EDIT_DIST_THRESHOLD:
                hit = None
                if ac_detuned:
                    artist_credit_name_p = ac_detuned
                    alt_try = True
                    continue

                if r_detuned:
                    recording_name_p = r_detuned
                    alt_try = True
                    continue

                if self.debug:
                    print("FAIL.\n");
            else:
                if alt_try:
                    self.good_alt += 1
                if self.debug:
                    print("OK.\n");


            break

        self.total += 1
        if not hit:
            self.bad += 1
            if not self.debug:
                print("F: %-60s %-60s" % (artist_credit_name[:59], recording_name[:59]))
            return None

        self.good += 1

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
                                          LIMIT 1000 OFFSET 510500""")
                for row in curs.fetchall():
                    self.search(row['artist_credit_name'], row['recording_name'])

mapping = MBIDMappingSearch()
mapping.lookup_rows()
mapping.report()
