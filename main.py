#!/usr/bin/env python3

from datasethoster.main import create_app, register_query
from msid_mapping import MSIDMappingQuery
from msid_lookup import MSIDLookupQuery
from artist_msid_lookup import ArtistMSIDLookupQuery
from ar_similarity import ArtistCreditSimilarityQuery
from lb_ar_similarity import ArtistSimilarityQuery
from rec_similarity import RecordingSimilarityQuery
from ac_name_lookup import ArtistCreditNameLookupQuery
from area_random_recording import AreaRandomRecordingQuery
from area_lookup import AreaLookupQuery
from genre_lookup import GenreLookupQuery
from top_discoveries import TopDiscoveriesQuery
from artist_country_from_artist_credit_id import ArtistCountryFromArtistCreditIdQuery
from releases_from_listen_stream import ReleasesFromListenStream
from top_new_tracks import TopNewTracksQuery
from top_missed_tracks import TopMissedTracksQuery
from bpm_key_from_recording import BPMKeyLookupQuery

register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())
register_query(ArtistMSIDLookupQuery())
register_query(ArtistCreditSimilarityQuery())
register_query(ArtistSimilarityQuery())
register_query(RecordingSimilarityQuery())
register_query(ArtistCreditNameLookupQuery())
register_query(AreaRandomRecordingQuery())
register_query(AreaLookupQuery())
register_query(GenreLookupQuery())
register_query(TopDiscoveriesQuery())
register_query(ArtistCountryFromArtistCreditIdQuery())
register_query(ReleasesFromListenStream())
register_query(TopNewTracksQuery())
register_query(TopMissedTracksQuery())
register_query(BPMKeyLookupQuery())

# Needs to be after register_query
app = create_app('config')

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
