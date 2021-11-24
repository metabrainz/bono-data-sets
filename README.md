build
-----

docker build -t metabrainz/bono-data-sets .

debug
-----

docker run -rm -p 8000:80 --name bono-data-sets --network musicbrainzdocker_default metabrainz/bono-data-sets

host
----

docker run -d -p 8000:80 --name bono-data-sets --network musicbrainzdocker_default metabrainz/bono-data-sets
