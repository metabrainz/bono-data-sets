#!/usr/bin/env python3

from datetime import datetime

from datasethoster import Query
from ago import human
import config
from release_lookup import listenbrainz_release_filter


class ReleasesFromListenStream(Query):
    """
        Given an LB user, work out which releases are in the listening stream.
    """

    def names(self):
        return ("releases-from-listen-stream", "ListenBrainz release from listen stream filter.")

    def inputs(self):
        return ['user', 'num_listens']

    def introduction(self):
        return """Given a ListenBrainz user name, parse the user's stream of listens (up to num_listens),
                  and find the releases the user listened to. num_listens = 1000 is a good starting point."""

    def outputs(self):
        return ['when', 'artist_credit_name', 'artist_credit_id', 'release_name', 'release_mbid', 'recording_names']

    def fetch(self, params, offset=-1, count=-1):
        user_name = params[0]['user']
        num_listens = int(params[0]['num_listens'])

        releases = listenbrainz_release_filter(user_name, num_listens)
        for release in releases:
            ts = release['listened_at']
            del release['listened_at']
            release['when'] = human(datetime.fromtimestamp(ts), precision=1)

        return releases
