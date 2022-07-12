#!/usr/bin/env python3

import datetime

import psycopg2
import psycopg2.extras
from datasethoster import Query

import config


class UpcomingReleasesQuery(Query):

    def names(self):
        return ("upcoming-releases", "MusicBrainz upcoming (and recent) releases")

    def inputs(self):
        return ['date', "days"]

    def introduction(self):
        return """This query shows releases that were released X days before and after the given date. Max number of days is 30. If no valid date is given, use today's date."""

    def outputs(self):
        return ["date", "artist_credit_name", "release_name", "release_mbid", "artist_mbids", "primary_type", "secondary_type"]

    def fetch(self, params, offset=-1, count=-1):

        try:
            dt = datetime.datetime.strptime(params[0]["date"], "%Y-%m-%d")
        except ValueError:
            dt = datetime.datetime.now()

        try:
            days = int(params[0]["days"])
        except ValueError:
            days = 15

        if days > 30:
            days = 30

        from_date = dt - datetime.timedelta(days=15)
        to_date = dt - datetime.timedelta(days=-15)

        with psycopg2.connect(config.DB_CONNECT_MB) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as curs:
                curs.execute("""SELECT release_mbid::TEXT
                                     , release_name
                                     , date
                                     , artist_credit_name
                                     , artist_mbids::TEXT[]
                                     , release_group_primary_type AS primary_type
                                     , release_group_secondary_type AS secondary_type
                                  FROM (
                                        SELECT DISTINCT rl.gid AS release_mbid
                                                      , rg.id AS release_group_id
                                                      , rl.name AS release_name
                                                      , make_date(rgm.first_release_date_year, rgm.first_release_date_month, rgm.first_release_date_day) AS date
                                                      , ac.name AS artist_credit_name
                                                      , array_agg(distinct a.gid) AS artist_mbids
                                                      , rgpt.name AS release_group_primary_type
                                                      , rgst.name AS release_group_secondary_type
                                                      , row_number() OVER (PARTITION BY rg.id ORDER BY make_date(rgm.first_release_date_year, rgm.first_release_date_month, rgm.first_release_date_day)) AS rnum
                                                  FROM release rl
                                                  JOIN release_group rg
                                                    ON rl.release_group = rg.id
                                                  JOIN release_group_meta rgm
                                                    ON rgm.id = rg.id
                                             LEFT JOIN release_group_primary_type rgpt
                                                    ON rg.type = rgpt.id
                                             LEFT JOIN release_group_secondary_type_join rgstj
                                                    ON rgstj.release_group = rg.id
                                             LEFT JOIN release_group_secondary_type rgst
                                                    ON rgstj.secondary_type = rgst.id
                                                  JOIN artist_credit ac
                                                    ON rl.artist_credit = ac.id
                                                  JOIN artist_credit_name acn
                                                    ON acn.artist_credit = ac.id
                                                  JOIN artist a
                                                    ON acn.artist = a.id
                                                 WHERE make_date(rgm.first_release_date_year, rgm.first_release_date_month, rgm.first_release_date_day) >= %s
                                                   AND make_date(rgm.first_release_date_year, rgm.first_release_date_month, rgm.first_release_date_day) <= %s
                                              GROUP BY rg.id, date, release_mbid, release_name, date, artist_credit_name, release_group_primary_type, release_group_secondary_type
                                        ) AS q
                                  WHERE q.rnum = 1
                               ORDER BY date, artist_credit_name, release_name""", (from_date, to_date))

                results = []
                while True:
                    data = curs.fetchone()
                    if not data:
                        break

                    results.append(dict(data))

                return results
