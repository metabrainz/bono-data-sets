import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

from troi.patches.artist_radio import LBRadioPatch
from troi.core import generate_playlist
from troi.playlist import LISTENBRAINZ_SERVER_URL
from datasethoster.exceptions import RedirectError, QueryError

class LBRadioQuery(Query):

    def names(self):
        return ("lb-radio", "ListenBrainz radio playlist generator")

    def inputs(self):
        return ['prompt', 'mode']

    def introduction(self):
        return f"""Generate an experimental LB radio playlist. Mode must be one of easy, medium or hard. Please
                   be patient, generating playlists takes a few moments! For docs, see 
                   https://gist.github.com/mayhem/9e8583848a40248d23c744c274ca2f6e 
                   """

    def outputs(self):
        return ['artist_name', 'recording_name', 'recording_mbid']

    def fetch(self, params, offset=-1, count=-1):

        print(f"'{params[0]['prompt']}'")

        patch = LBRadioPatch()
        try:
            playlist = generate_playlist(patch, args={ "mode": params[0]["mode"],
                "prompt": params[0]["prompt"],
                "token": config.TROI_BOT_TOKEN,
                "upload": True})
        except RuntimeError as err:
            raise QueryError(err)

        if playlist is None:
            raise QueryError("""No playlist was generated -- the prompt did not generate any matching recordings.""")

        results = []
        for r in playlist.playlists[0].recordings:
            results.append({ "recording_mbid": r.mbid,
                             "artist_name": r.artist.name,
                             "recording_name": r.name })

        raise RedirectError(LISTENBRAINZ_SERVER_URL + "/playlist/" + playlist.playlists[0].mbid)
