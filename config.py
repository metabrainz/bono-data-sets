#
# Database connection.

# If you are connecting to a musicbrainz-docker instance for MusicBrainz data, this string should work:
DB_CONNECT_MB = "dbname=musicbrainz_db user=musicbrainz host=localhost port=25432 password=musicbrainz"
DB_CONNECT_MSB = "dbname=messybrainz user=messybrainz host=localhost port=25432 password=messybrainz"

# For use in Docker
DB_CONNECT_MB = "dbname=musicbrainz_db user=musicbrainz host=db port=5432 password=musicbrainz"

# For use with typesense
TYPESENSE_API_KEY = "root-api-key"
