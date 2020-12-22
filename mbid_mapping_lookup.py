#!/usr/bin/env python3

import re
import socket
from time import sleep
import ujson

import psycopg2
import psycopg2.extras
from psycopg2.extras import execute_values
import typesense
import typesense.exceptions
import requests.exceptions
from datasethoster import Query
from datasethoster.main import app, register_query
from unidecode import unidecode
from Levenshtein import distance

import config

BATCH_SIZE = 100

def prepare_query(text):
    return unidecode(re.sub(" +", " ", re.sub(r'[^\w ]+', '', text)).lower())

def insert_rows(curs, table, values):
    '''
        Helper function to insert a large number of rows into postgres in one go.
    '''

    query = "INSERT INTO " + table + " VALUES %s"
    execute_values(curs, query, values, template=None)

class MBIDMappingSearch(Query):

    EDIT_DIST_THRESHOLD = 5

    def __init__(self):
        self.good = 0
        self.good_alt = 0
        self.bad = 0
        self.total = 0
        self.debug = False

        self.client = typesense.Client({
            'nodes': [{
              'host': config.TYPESENSE_HOST,
              'port': '8108',
              'protocol': 'http',
            }],
            'api_key': config.TYPESENSE_API_KEY,
            'connection_timeout_seconds': 10
        })

    def report(self):
        if self.total:
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

        while True:
            try:
                hits = self.client.collections['mbid_mapping'].documents.search(search_parameters)
                break
            except requests.exceptions.ReadTimeout:
                print("Got socket timeout, sleeping 5 seconds, trying again.")
                sleep(5)

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

        return [hit['document']['artist_credit_name'],
                hit['document']['artist_credit_id'],
                hit['document']['release_name'],
                hit['document']['release_mbid'],
                hit['document']['recording_name'],
                hit['document']['recording_mbid']]


    def lookup_rows(self):

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with psycopg2.connect(config.DB_CONNECT_MB) as ins_conn:
                with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                    with ins_conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as ins_curs:
                        curs.execute("""SELECT DISTINCT fl.recording_msid, fl.artist_credit_name, fl.recording_name 
                                                   FROM mapping.first_listened_2020 fl
                                              LEFT JOIN mapping.first_listened_2020_mapping m
                                                     ON fl.recording_msid = m.recording_msid
                                                  WHERE m.recording_msid is null
                                               ORDER BY fl.artist_credit_name, fl.recording_name""")
                        results = []
                        for row in curs.fetchall():
                            hit = self.search(row['artist_credit_name'], row['recording_name'])
                            if hit:
                                hit.insert(0, row["recording_msid"])
                                ins_conn.commit()
                                results.append(hit)

                            if len(results) == BATCH_SIZE:
                                insert_rows(ins_curs, "mapping.first_listened_2020_mapping", results)
                                results = []
                                self.report()

                        if len(results):
                            insert_rows(ins_curs, "mapping.first_listened_2020_mapping", results)
                            ins_conn.commit()

ignore_me = """
 create table mapping.first_listened_2020_mapping (recording_msid TEXT NOT NULL,
                                                   artist_credit_name TEXT NOT NULL, 
                                                   artist_credit_id INTEGER NOT NULL, 
                                                   release_name TEXT NOT NULL, 
                                                   release_mbid TEXT NOT NULL, 
                                                   recording_name TEXT NOT NULL, 
                                                   recording_mbid TEXT NOT NULL);
create index first_listened_2020_mapping_ndx_recording_msid on mapping.first_listened_2020_mapping ( recording_msid);
"""

mapping = MBIDMappingSearch()
mapping.lookup_rows()
mapping.report()
