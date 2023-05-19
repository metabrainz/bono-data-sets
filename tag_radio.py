import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

from troi.patches.tag_radio import TagRadioPatch
from troi.core import generate_playlist
from troi.playlist import LISTENBRAINZ_SERVER_URL
from datasethoster.exceptions import RedirectError, QueryError

class TagRadioQuery(Query):

    def names(self):
        return ("tag-radio", "Tag-radio type playlist generator")

    def inputs(self):
        return ['[tags]']

    def introduction(self):
        return """Generate an experimental tag-radio playlist."""

    def outputs(self):
        return ['artist_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=-1, count=-1):

        tags = tuple([ p['[tags]'].lower().strip() for p in params ])

        patch = TagRadioPatch()
        try:
            playlist = generate_playlist(patch, args={
                "tags": tags,
                "token": config.TROI_BOT_TOKEN,
                "upload": True,
                "user_name": "-"})
        except RuntimeError as err:
            raise QueryError(err)

        if playlist is None:
            raise QueryError("""No playlist was generated -- we likely do not have enough information to generate a
                                playlist for one of the tags.""")

        results = []
        for r in playlist.playlists[0].recordings:
            results.append({ "recording_mbid": r.mbid,
                             "artist_name": r.artist.name,
                             "recording_name": r.name })

        raise RedirectError(LISTENBRAINZ_SERVER_URL + "/playlist/" + playlist.playlists[0].mbid)
