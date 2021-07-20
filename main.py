#!/usr/bin/env python3

from datasethoster.main import create_app, register_query
from msid_mapping import MSIDMappingQuery
from msid_lookup import MSIDLookupQuery
from artist_msid_lookup import ArtistMSIDLookupQuery
from ar_similarity import ArtistCreditSimilarityQuery
from ac_name_lookup import ArtistCreditNameLookupQuery
from area_random_recording import AreaRandomRecordingQuery
from area_lookup import AreaLookupQuery
from genre_lookup import GenreLookupQuery
from first_listened_2020 import FirstListenedIn2020Query
from artist_country_from_artist_credit_id import ArtistCountryFromArtistCreditIdQuery

register_query(MSIDMappingQuery())
register_query(MSIDLookupQuery())
register_query(ArtistMSIDLookupQuery())
register_query(ArtistCreditSimilarityQuery())
register_query(ArtistCreditNameLookupQuery())
register_query(AreaRandomRecordingQuery())
register_query(AreaLookupQuery())
register_query(GenreLookupQuery())
register_query(FirstListenedIn2020Query())
register_query(ArtistCountryFromArtistCreditIdQuery())

app = create_app('config')

# Needs to be after register_query
app = create_app('config')

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
