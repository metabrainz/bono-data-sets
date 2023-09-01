POPULAR_TAGS = [
    "rock", 
    "electronic", 
    "pop", 
    "jazz", 
    "experimental", 
    "hip hop", 
    "classical", 
    "ambient", 
    "punk", 
    "alternative rock", 
    "metal", 
    "pop rock", 
    "indie rock", 
    "folk", 
    "house", 
    "heavy metal", 
    "blues", 
    "downtempo", 
    "techno", 
    "indie", 
    "synth-pop", 
    "country", 
    "sou",
    "hard rock", 
    "electro", 
    "pop/rock", 
    "industrial", 
    "classic rock", 
    "dance", 
    "folk rock", 
    "soundtrack", 
    "black metal", 
    "instrumental", 
    "death metal", 
    "r&b", 
    "psychedelic rock", 
    "reggae", 
    "trance", 
    "noise", 
    "progressive rock", 
    "latin", 
    "rock and roll", 
    "new wave", 
    "idm", 
    "funk", 
    "alternative", 
    "blues rock", 
    "ballad", 
    "disco", 
    "singer-songwriter", 
    "dark ambient", 
    "lo-fi", 
    "post-rock", 
    "modern classical", 
    "drone", 
    "electronica", 
    "indie pop", 
    "rockabilly", 
    "post-punk", 
    "drum and bass", 
    "soft rock", 
    "rap", 
    "hardcore", 
    "schlager", 
    "doom metal", 
    "easy listening", 
    "dub", 
    "contemporary jazz", 
    "ebm", 
    "score", 
    "psychedelic", 
    "rock & roll", 
    "minimal", 
    "avant-garde", 
    "country rock", 
    "dubstep", 
    "trip hop", 
    "swing", 
    "shoegaze", 
    "alternative/indie rock", 
    "vocal", 
    "deep house", 
    "garage rock", 
    "dark wave", 
    "non-music", 
    "art rock", 
    "progressive house", 
    "chillout", 
    "contemporary pop/rock", 
    "breakbeat", 
    "acoustic rock", 
    "stoner rock", 
    "punk rock", 
    "tech house", 
    "synthwave", 
    "new age", 
    "vaporwave", 
    "europop", 
    "euro house", 
    "leftfield", 
    "big band", 
    "progressive trance", 
    "ska", 
    "chillwave", 
    "progressive metal", 
    "hip-hop", 
    "album rock", 
    "thrash metal", 
    "breaks", 
    "contemporary classical", 
    "elektro", 
    "romantic classical", 
    "comedy", 
    "alternative pop/rock", 
    "j-pop", 
    "baroque", 
    "abstract electronic", 
    "fusion", 
    "christmas", 
    "instrumental hip hop", 
    "grindcore", 
    "bluegrass", 
    "emo", 
    "pop rap", 
    "adult contemporary", 
    "chanson française", 
    "boom bap", 
    "club", 
    "roots reggae", 
    "power pop", 
    "grunge", 
    "vocal jazz", 
    "dance-pop", 
    "movie soundtrack", 
    "alternative metal", 
    "jazz-funk", 
    "goth rock", 
    "power metal", 
    "lo-fi hip hop", 
    "pop punk", 
    "dream pop", 
    "united kingdom", 
    "smooth jazz", 
    "hip-hop/rap", 
    "rock/goth rock", 
    "classic country", 
    "glitch", 
    "london", 
    "opera", 
    "punk/new wave", 
    "gangsta rap", 
    "orchestral", 
    "gothic", 
    "bebop", 
    "contemporary country", 
    "electropop", 
    "psytrance", 
    "acoustic", 
    "los angeles", 
    "concerto", 
    "contemporary r&b", 
    "americana", 
    "celtic", 
    "synthpop", 
    "electric blues", 
    "gospel", 
    "contemporary folk", 
    "filk", 
    "symphony", 
    "symphonic rock", 
    "conscious hip hop", 
    "alternative country", 
    "new york", 
    "arena rock", 
    "southern rock", 
    "space rock", 
    "neofolk", 
    "chamber music", 
    "doom", 
    "free improvisation", 
    "traditional country", 
    "metalcore", 
    "cool jazz", 
    "britpop", 
    "dancehall", 
    "world", 
    "japan", 
    "free jazz", 
    "underground hip hop", 
    "gothic metal", 
    "post-bop", 
    "hard bop", 
    "doo-wop", 
    "progressive", 
    "soul jazz", 
    "keyboard", 
    "nu metal", 
    "chiptune", 
    "jungle", 
    "jazz rock", 
    "future jazz", 
    "speed metal", 
    "hard trance", 
    "motown", 
    "industrial metal", 
    "hardcore rap", 
    "gothic rock", 
    "traditional", 
    "experimental electronic",
    "synth", 
    "soundscape", 
    "east coast hip hop", 
    "christian", 
    "atmospheric", 
    "alternative and punk", 
    "melodic death metal", 
    "latin jazz", 
    "darkwave", 
]

POPULAR_TAGS_QUERY = """
WITH counts AS (
            SELECT t.name, count(at.tag) as cnt
              FROM artist a
              JOIN artist_tag at
                ON at.artist = a.id
              JOIN tag t
                ON at.tag = t.id
          GROUP BY t.name
            HAVING t.count > 0
        UNION
            SELECT t.name, count(rt.tag) as cnt
              FROM release r
              JOIN release_tag rt
                ON rt.release = r.id
              JOIN tag t
                ON rt.tag = t.id
          GROUP BY t.name
            HAVING t.count > 0
        UNION
            SELECT t.name, count(rgt.tag) as cnt
              FROM release_group rg
              JOIN release_group_tag rgt
                ON rgt.release_group = rg.id
              JOIN tag t
                ON rgt.tag = t.id
          GROUP BY t.name
            HAVING t.count > 0
        UNION
            SELECT t.name, count(rect.tag) as cnt
              FROM recording rec
              JOIN recording_tag rect
                ON rect.recording = rec.id
              JOIN tag t
                ON rect.tag = t.id
          GROUP BY t.name
            HAVING t.count > 0
)           
          SELECT c.name, sum(c.cnt) as cnt
            FROM counts c
           WHERE cnt > 10000
        GROUP BY c.name  
        ORDER BY cnt DESC;  
"""
