# -*- coding: utf-8 -*-
import json
import sqlite3 as sqlite
import sys

genre = str(sys.argv[1])
n = int(sys.argv[2])
print 'Top {} actors who played in most {} movies:'.format(n,genre)
print 'Actor, {} Movies Played in'.format(genre)
with sqlite.connect(r'hw3.db') as con:
    cur = con.cursor()
    cur.execute("SELECT movie_actor.actor, Count(*), movie_genre.genre FROM movie_actor JOIN movie_genre WHERE movie_actor.imdb_id=movie_genre.imdb_id and movie_genre.genre=\'{}\' GROUP BY movie_actor.actor ORDER BY Count(*) DESC LIMIT 0,{}".format(genre,n))
    rows = cur.fetchall()
    for row in rows:
        print row[0]+', '+str(row[1])
