#!/usr/bin/env python3

from datasethoster.main import create_app, register_query
from ac_name_lookup import ArtistCreditNameLookupQuery
from area_random_recording import AreaRandomRecordingQuery
from area_lookup import AreaLookupQuery
#from genre_lookup import GenreLookupQuery
from artist_country_from_artist_credit_id import ArtistCountryFromArtistCreditIdQuery

register_query(ArtistCreditNameLookupQuery())
register_query(AreaRandomRecordingQuery())
register_query(AreaLookupQuery())
#register_query(GenreLookupQuery())
register_query(ArtistCountryFromArtistCreditIdQuery())

# Needs to be after register_query
app = create_app('config')

if __name__ == "__main__":
    app.debug = True
    app.run(host="0.0.0.0", port=4201)
