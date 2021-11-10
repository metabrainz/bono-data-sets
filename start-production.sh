#!/bin/bash

DS_DOMAIN="bono.metabrainz.org"
SIM_DOMAIN="similarity.acousticbrainz.org"

echo "---- start bono-data-sets"
docker run -d -p 8000:80 \
    --name bono-data-sets \
    --network musicbrainzdocker_default \
    --env "LETSENCRYPT_HOST=$DS_DOMAIN" \
    --env "LETSENCRYPT_EMAIL=rob@metabrainz.org" \
    --env "VIRTUAL_HOST=$DS_DOMAIN" \
    metabrainz/bono-data-sets

echo "---- start nginx proxy, le"
docker run -d -p 80:80 -p 443:443 \
   --name nginx \
   -v /etc/ssl/le-certs:/etc/nginx/certs:ro \
   -v /etc/nginx/vhost.d \
   -v /usr/share/nginx/html \
   -v /var/run/docker.sock:/tmp/docker.sock:ro \
   -v `pwd`/nginx/proxy:/etc/nginx/vhost.d/$DS_DOMAIN:ro \
   -v `pwd`/nginx/proxy:/etc/nginx/vhost.d/$SIM_DOMAIN:ro \
   --label com.github.jrcs.letsencrypt_nginx_proxy_companion.nginx_proxy \
   --restart unless-stopped \
   --network=musicbrainzdocker_default \
   jwilder/nginx-proxy

docker run -d \
   --name le \
   -v /var/run/docker.sock:/var/run/docker.sock:ro \
   -v /etc/ssl/le-certs:/etc/nginx/certs:rw \
   --volumes-from nginx \
   --restart unless-stopped \
   --network=musicbrainzdocker_default \
   jrcs/letsencrypt-nginx-proxy-companion
