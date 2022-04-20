# Hosting bono-data-sets

In order to host the data set hoster on wolf, follow the instructions below. For more information
on the dev server, see SysWiki DevelopmentServer.md .


## build

docker-compose build

## debug

See the comments in config.py about how to setup DB connect strings to run a test server, then:

```
python3 -v venv .ve 
source .ve/bin/activate
pip3 install -r requirements.tx
./main.py
```

## host

To host a container to run indefinitely on the Development Server, do this:

```
docker-compose run -d
```
