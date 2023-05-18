import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

from troi.patches.artist_radio import ArtistRadioPatch
from troi.core import generate_playlist
from troi.playlist import LISTENBRAINZ_SERVER_URL
from datasethoster.exceptions import RedirectError, QueryError

class ArtistRadioQuery(Query):

    def names(self):
        return ("artist-radio", "Artist-radio type playlist generator")

    def inputs(self):
        return ['[artist_mbid]', 'mode']

    def introduction(self):
        return """Generate an experimental artist-radio playlist. Mode must be one of easy, medium or hard. Please
                  be patient, generating playlists takes a few moments!"""

    def outputs(self):
        return ['artist_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=-1, count=-1):

        mode = params[0]["mode"]
        artist_mbids = tuple([ p['[artist_mbid]'].lower() for p in params ])

        patch = ArtistRadioPatch()
        try:
            playlist = generate_playlist(patch, args={ "mode": mode,
                "artist_mbid": artist_mbids,
                "token": config.TROI_BOT_TOKEN,
                "upload": True})
        except RuntimeError as err:
            raise QueryError(err)

        if playlist is None:
            raise QueryError("""No playlist was generated -- we likely do not have enough information to generate a
                                playlist for one of the seed artists.""")

        results = []
        for r in playlist.playlists[0].recordings:
            results.append({ "recording_mbid": r.mbid,
                             "artist_name": r.artist.name,
                             "recording_name": r.name })

        raise RedirectError(LISTENBRAINZ_SERVER_URL + "/playlist/" + playlist.playlists[0].mbid)
