import psycopg2
import psycopg2.extras
from datasethoster import Query
import config

from troi.patches.lb_radio import LBRadioPatch
from troi.core import generate_playlist
from troi.playlist import LISTENBRAINZ_SERVER_URL
from datasethoster.exceptions import RedirectError, QueryError

class LBRadioQuery(Query):

    def __init__(self):
        self.playlist_name = ""
        self.playlist_desc = ""

    def names(self):
        return ("lb-radio", "ListenBrainz radio playlist generator")

    def inputs(self):
        return ['prompt', 'mode']

    def introduction(self):
        return f"""Generate an experimental LB radio playlist. Mode must be one of easy, medium or hard. Please
                   be patient, generating playlists takes a few moments! For docs, see 
                   <a href="https://troi.readthedocs.io/en/add-user-stats-entity/lb_radio.html">LB Radio documentation</a>.
                   """

    def outputs(self):
        return None

    def additional_data(self):
        return { "playlist_name": self.playlist_name, "playlist_desc": self.playlist_desc }

    def fetch(self, params, offset=-1, count=-1):

        patch = LBRadioPatch()
        try:
            playlist = generate_playlist(patch, args={ "mode": params[0]["mode"], "prompt": params[0]["prompt"]})
        except RuntimeError as err:
            raise QueryError(err)

        data = []
        feedback = []
        if playlist is None:
            feedback.append("""No playlist was generated -- the prompt did not generate any matching recordings.""")
        else:
            self.playlist_name = playlist.playlists[0].name
            self.playlist_desc = playlist.playlists[0].description
            for r in playlist.playlists[0].recordings:
                data.append({ "recording_mbid": r.mbid,
                              "artist_name": r.artist.name,
                              "recording_name": r.name })

        result = []
        feedback.extend(patch.user_feedback())
        if feedback is not None:
            markup = "<h3>User Feedback</h3><ul>"
            for feedback in feedback:
                markup += f"<li>{ feedback}</li>"
            markup += "</ul>"
            result.append(
                {
                    "type": "markup",
                    "data": markup
                })

        result.append(
            {
                "type": "dataset",
                "columns": ['artist_name', 'recording_name', 'recording_mbid'],
                "data": data,
            }
        )
        return result
