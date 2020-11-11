#
# Database connection.

# If you are connecting to a musicbrainz-docker instance for MusicBrainz data, this string should work:
DB_CONNECT_MB = "dbname=musicbrainz_db user=musicbrainz host=localhost port=25432 password=musicbrainz"
DB_CONNECT_MSB = "dbname=messybrainz user=messybrainz host=localhost port=25432 password=messybrainz"

# For use in Docker
#DB_CONNECT_MB = "dbname=musicbrainz_db user=musicbrainz host=db port=5432 password=musicbrainz"

# Mapping specific settings

# For debugging, only fetches a tiny portion of the data if True
USE_MINIMAL_DATASET = True  

# Turn this off during debugging, it makes the test data easier to read
REMOVE_NON_WORD_CHARS = True  

# Show matches as the algorithms go
SHOW_MATCHES = False  
