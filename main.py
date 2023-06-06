#!/usr/bin/env python3

import psycopg2.extras

from datasethoster.main import create_app, register_query
from ac_name_lookup import ArtistCreditNameLookupQuery
from area_random_recording import AreaRandomRecordingQuery
from area_lookup import AreaLookupQuery
#from genre_lookup import GenreLookupQuery
from artist_country_from_artist_credit_id import ArtistCountryFromArtistCreditIdQuery
from mb_canonical_data import MusicBrainzCanonicalDataLookup
#from rec_similarity import RecordingSimilarityQuery
from upcoming_releases import UpcomingReleasesQuery
from mb_canonical_data import MusicBrainzCanonicalDataLookup
from releases_from_recording import ReleasesFromRecordingQuery
from top_discoveries import TopDiscoveriesQuery
from popular_recordings import PopularRecordingsByArtistQuery
from artist_lookup import ArtistLookupQuery
from artist_radio import ArtistRadioQuery
from recording_from_tags import RecordingFromTagsQuery
from recording_from_rg_tags import RecordingFromReleaseGroupTagsQuery
from tag_radio import TagRadioQuery
from rg_tag_radio import ReleaseGroupTagRadioQuery
from popular_releases_by_listencount import PopularReleasesFromTotalListenCountQuery
from popular_releases_by_listeners import PopularReleasesFromTotalListenersQuery

psycopg2.extras.register_uuid()

register_query(MusicBrainzCanonicalDataLookup())
register_query(AreaRandomRecordingQuery())
register_query(AreaLookupQuery())
#register_query(GenreLookupQuery())
register_query(ArtistCountryFromArtistCreditIdQuery())
register_query(MusicBrainzCanonicalDataLookup())
#register_query(RecordingSimilarityQuery())
register_query(UpcomingReleasesQuery())
register_query(ReleasesFromRecordingQuery())
register_query(TopDiscoveriesQuery())
register_query(PopularRecordingsByArtistQuery())
register_query(ArtistLookupQuery())
register_query(ArtistRadioQuery())
register_query(RecordingFromTagsQuery())
register_query(RecordingFromReleaseGroupTagsQuery())
register_query(TagRadioQuery())
register_query(ReleaseGroupTagRadioQuery())
register_query(PopularReleasesFromTotalListenCountQuery())
register_query(PopularReleasesFromTotalListenersQuery())

# Needs to be after register_query
app = create_app('config')

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
