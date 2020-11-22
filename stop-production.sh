#!/bin/bash


echo "---- stop proxy, le"
docker stop nginx le
docker rm nginx le

echo "---- stop bono-data-sets"
docker stop bono-data-sets
docker rm bono-data-sets

echo "---- done"
