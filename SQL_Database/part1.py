# -*- coding: utf-8 -*-

import json
import sqlite3 as sqlite
import sys

# parse json file
with open(r'movie_actors_data.txt','r') as f:
    movie_genre = []
    movies = []
    movie_actor = []
    for line in f.readlines():
        line = json.loads(line)
        imdb_id = line['imdb_id']
        genre = line['genres']
        title = line['title']
        year = line['year']
        rating = line['rating']
        actor = line['actors']
        if len(genre) != 0:
            for i in genre:
                movie_genre.append((imdb_id,i))
        movies.append((imdb_id,title,year,rating))
        for i in actor:
            movie_actor.append((imdb_id,i))

# create tables
with sqlite.connect(r'hw3.db') as con:
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS movie_genre")
    cur.execute("CREATE TABLE movie_genre (imdb_id TEXT, genre TEXT)")
    cur.executemany("INSERT INTO movie_genre VALUES(?,?)", movie_genre)
    cur.execute("DROP TABLE IF EXISTS movies")
    cur.execute("CREATE TABLE movies (imdb_id TEXT, title TEXT, year INT, rating REAL)")
    cur.executemany("INSERT INTO movies VALUES(?,?,?,?)", movies)
    cur.execute("DROP TABLE IF EXISTS movie_actor")
    cur.execute("CREATE TABLE movie_actor (imdb_id TEXT, actor TEXT)")
    cur.executemany("INSERT INTO movie_actor VALUES(?,?)", movie_actor)
    con.commit()
    # sql queries
    cur.execute("SELECT genre, Count(*) FROM movie_genre GROUP BY genre ORDER BY Count(*) DESC LIMIT 0,10")
    rows = cur.fetchall()
    print 'Top 10 genres:'
    print 'Genre, Movies'
    for row in rows:
        print row[0]+', '+str(row[1])
    print
    print
    cur.execute("SELECT year, Count(*) FROM movies GROUP BY year ORDER BY year")
    rows = cur.fetchall()
    print 'Movies broken down by year:'
    print 'Year, Movies'
    for row in rows:
        print str(row[0])+', '+str(row[1])
    print
    print
    cur.execute("SELECT movies.title, movies.year, movies.rating, movie_genre.genre FROM movies JOIN movie_genre WHERE movie_genre.imdb_id=movies.imdb_id and movie_genre.genre='Sci-Fi' ORDER BY rating DESC, year DESC")
    rows = cur.fetchall()
    print 'Sci-Fi movies:'
    print 'Title, Year, Rating'
    for row in rows:
        print row[0]+', '+str(row[1])+', '+str(row[2])
    print
    print
    cur.execute("SELECT movie_actor.actor, Count(*), movies.year FROM movies JOIN movie_actor WHERE movie_actor.imdb_id=movies.imdb_id and movies.year>=2000 GROUP BY movie_actor.actor ORDER BY Count(*) DESC, movie_actor.actor LIMIT 0,10")
    rows = cur.fetchall()
    print 'In and after year 2000, top 10 actors who played in most movies:'
    print 'Actor, Movies'
    for row in rows:
        print row[0]+', '+str(row[1])
    print
    print
    cur.execute("SELECT DISTINCT a.actor, b.actor, Count(*) FROM movie_actor a INNER JOIN movie_actor b WHERE a.imdb_id=b.imdb_id and a.actor<b.actor GROUP BY a.actor, b.actor HAVING Count(*)>=3 ORDER BY Count(*) DESC")
    rows = cur.fetchall()
    print 'Pairs of actors who co-stared in 3 or more movies:'
    print 'Actor A, Actor B, Co-stared Movies'
    for row in rows:
        print row[0]+', '+row[1]+', '+str(row[2])
