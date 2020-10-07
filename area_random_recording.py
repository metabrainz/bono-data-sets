from operator import itemgetter

import psycopg2
import psycopg2.extras
from datasethoster import Query
from datasethoster.main import app, register_query
import config

DEFAULT_ROWS = 50

class AreaRandomRecordingQuery(Query):

    def names(self):
        return ("area-random-recordings", "Select a random set of recordings from artists for a given country.")

    def inputs(self):
        return ['area_id', 'start_year', 'end_year']

    def introduction(self):
        return """Given a country (area) randomly select a number of recordings for that country."""

    def outputs(self):
        return ['recording_mbid', 'recording_name', 'artist_credit_id', 'artist_credit_name', 'year']

    def fetch(self, params, offset=-1, count=DEFAULT_ROWS):

        if count < 1:
            count = DEFAULT_ROWS

        try:
            area = int(params[0]['area_id'])
            start_year = int(params[0]['start_year'])
            end_year = int(params[0]['end_year'])
        except ValueError:
            raise BadRequest("area_id, start_year and end_year should all be integers")

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                query = """SELECT r.gid AS recording_mbid, 
                                  r.name AS recording_name, 
                                  ac.id AS artist_credit_id, 
                                  ac.name AS artist_credit_name,
                                  date_year AS year
                             FROM recording r
                             JOIN artist_credit ac
                               ON r.artist_credit = ac.id
                             JOIN track t
                               ON r.id = t.recording
                             JOIN medium m
                               ON t.medium = m.id
                             JOIN release rl
                               ON m.release = rl.id
                             JOIN release_country rc
                               ON rc.release = rl.id
                            WHERE r.artist_credit IN
                                      (SELECT DISTINCT acn.artist_credit
                                         FROM artist a
                                         JOIN artist_credit_name acn
                                           ON acn.artist = a.id
                                        WHERE a.area = %s
                                        LIMIT 100)
                              AND rc.date_year >= %s
                              AND rc.date_year <= %s
                            ORDER BY random()"""

                args = [area, start_year, end_year]
                if count > 0:
                    query += " LIMIT %s"
                    args.append(count)
                if offset >= 0:
                    query += " OFFSET %s"
                    args.append(offset)

                curs.execute(query, tuple(args))
                acs = []
                while True:
                    row = curs.fetchone()
                    if not row:
                        break

                    r = dict(row)
                    r['recording_mbid'] = str(r['recording_mbid'])
                    acs.append(dict(row))

                return acs
