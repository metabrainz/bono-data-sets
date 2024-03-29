#!/usr/bin/env python3

import psycopg2.extras

from datasethoster.main import create_app, register_query
from ac_name_lookup import ArtistCreditNameLookupQuery
from area_random_recording import AreaRandomRecordingQuery
from area_lookup import AreaLookupQuery
from artist_country_from_artist_credit_id import ArtistCountryFromArtistCreditIdQuery
from mb_canonical_data import MusicBrainzCanonicalDataLookup
from upcoming_releases import UpcomingReleasesQuery
from mb_canonical_data import MusicBrainzCanonicalDataLookup
from releases_from_recording import ReleasesFromRecordingQuery
from artist_lookup import ArtistLookupQuery
from popular_releases_by_listencount import PopularReleasesFromTotalListenCountQuery
from popular_releases_by_listeners import PopularReleasesFromTotalListenersQuery
from popular_artists_by_listencount import PopularArtistsFromTotalListenCountQuery
from popular_artists_by_listeners import PopularArtistsFromTotalListenersQuery
from popular_recordings_by_listeners import PopularRecordingsFromTotalListenersQuery
from popular_recordings_by_listencount import PopularRecordingsFromTotalListenCountQuery
from feedback_lookup import FeedbackLookupQuery
from similar import SimilarArtistSelectorQuery
#from bulk_recording_lookup import BulkRecordingLookupQuery

psycopg2.extras.register_uuid()

register_query(MusicBrainzCanonicalDataLookup())
register_query(AreaRandomRecordingQuery())
register_query(AreaLookupQuery())
register_query(ArtistCountryFromArtistCreditIdQuery())
register_query(MusicBrainzCanonicalDataLookup())
register_query(UpcomingReleasesQuery())
register_query(ReleasesFromRecordingQuery())
register_query(ArtistLookupQuery())
register_query(PopularReleasesFromTotalListenCountQuery())
register_query(PopularReleasesFromTotalListenersQuery())
register_query(PopularArtistsFromTotalListenCountQuery())
register_query(PopularArtistsFromTotalListenersQuery())
register_query(PopularRecordingsFromTotalListenersQuery())
register_query(PopularRecordingsFromTotalListenCountQuery())
register_query(FeedbackLookupQuery())
register_query(SimilarArtistSelectorQuery())

# Needs to be after register_query
app = create_app('config')

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
