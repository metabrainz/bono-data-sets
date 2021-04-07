import re

import typesense
import typesense.exceptions
from datasethoster import Query
from unidecode import unidecode

import config

def prepare_query(text):
    return unidecode(re.sub(" +", " ", re.sub(r'[^\w ]+', '', text)).lower())

class ArtistCreditRecordingMappingSearchQuery(Query):

    def names(self):
        return ("acrm-search", "MusicBrainz Artist Credit Recording Mapping search")

    def inputs(self):
        return ['query']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a (potentially fuzzy) match in MusicBrainz. Construct
                  the search query by combining artist name and recording name. (e.g. 'portishead strangers')"""

    def outputs(self):
        return ['recording_name', 'recording_mbid',
                'release_name', 'release_mbid',
                'artist_credit_name', 'artist_credit_id']

    def fetch(self, params, offset=-1, count=-1):

        query = params[0]['query']

        client = typesense.Client({
            'nodes': [{
              'host': config.TYPESENSE_HOST,
              'port': '8108',
              'protocol': 'http',
            }],
            'api_key': config.TYPESENSE_API_KEY,
            'connection_timeout_seconds': 2
        })

        search_parameters = {
            'q'         : prepare_query(query),
            'query_by'  : "combined",
            'prefix'    : 'no',
            'num_typos' : 5
        }

        hits = client.collections['mbid_mapping_latest'].documents.search(search_parameters)

        output = []
        for hit in hits['hits']:
            output.append({ 'artist_credit_name': hit['document']['artist_credit_name'],
                            'artist_credit_id': hit['document']['artist_credit_id'],
                            'release_name': hit['document']['release_name'],
                            'release_mbid': hit['document']['release_mbid'],
                            'recording_name': hit['document']['recording_name'],
                            'recording_mbid': hit['document']['recording_mbid'] })

        return output
