import ujson

import typesense
import typesense.exceptions
from datasethoster import Query
from datasethoster.main import app, register_query
from unidecode import unidecode

import config


class ArtistCreditRecordingMappingSearchQuery(Query):

    def names(self):
        return ("acrm-search", "MusicBrainz Artist Credit Recording Mapping search")

    def inputs(self):
        return ['query', 'query_by']

    def introduction(self):
        return """This page allows you to enter the name of an artist and the name of a recording (track)
                  and the query will attempt to find a potentially fuzzy match in MusicBrainz. Available fields
                  in the DB are: 'artist_credit', 'recording' or 'combined'. One ore more of these fields
                  must be specified in the query_by field."""

    def outputs(self):
        return ['artist_credit_name', 'recording_name']

    def fetch(self, params, offset=-1, count=-1):

        query = params[0]['query']
        query_by = params[0]['query_by'] or "artist_credit_name,recording_name"

        client = typesense.Client({
            'nodes': [{
              'host': 'typesense',
              'port': '8108',
              'protocol': 'http',
            }],
            'api_key': config.TYPESENSE_API_KEY,
            'connection_timeout_seconds': 2
        })

        search_parameters = {
            'q'         : query,
            'query_by'  : query_by,
            'prefix'    : 'no',
            'num_typos' : 5
        }

        hits = client.collections['recording_artist_credit_pairs'].documents.search(search_parameters)

        output = []
        for hit in hits['hits']:
            output.append({ 'artist_credit_name': hit['document']['artist_credit'],
                            'recording_name': hit['document']['recording'] })

        return output
