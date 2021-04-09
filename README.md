build
-----

docker build -t metabrainz/mbid-mapping-hoster .

debug
-----

docker run -rm -p 8000:80 --name mbid-mapping-hoster --network musicbrainzdocker_default metabrainz/mbid-mapping-hoster

host
----

docker run -d -p 8000:80 --name mbid-mapping-hoster --network musicbrainzdocker_default metabrainz/mbid-mapping-hoster
