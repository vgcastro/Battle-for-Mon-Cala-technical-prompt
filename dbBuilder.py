import sqlite3
import csv

def loadTable(connector, tableNAme, rowItems, fileName):
    """This function creates a table from the information provided by the IMDB file"""
    #cursor object
    imdbCursor = connector.cursor()
    #build the table
    imdbCursor.execute(("CREATE TABLE " + tableNAme)) # movies(movieId PRIMARY KEY, movieName)
    #Fill out the entry for values in the row construction statement
    rowEntryCount = len(rowItems)
    valueAppend = rowEntryCount * '?,'
    valueEntrySkeleton = valueAppend[0:((rowEntryCount * 2) -1)]
    rowConstruction = 'INSERT INTO ' + tableNAme + " VALUES" + '(' + valueEntrySkeleton + ')'
    #put in the columns by first opening rows and then putting in the rows with the row numbers chosen by the row constructor
    with open(fileName) as tsvFile:
        tsvReader = csv.reader(tsvFile, delimiter='\t')
        counter = 0
        for row in tsvReader:
            if counter > 0:
                newRowSkeleton = rowConstruction.replace(' PRIMARY KEY', '')
                entry = constructEntry(row, rowItems)
                imdbCursor.execute(newRowSkeleton, entry)
            counter += 1
    #commit to the database
    connector.commit()


def dbConnect():
    """This function connects to the databse. If it is the first time, it will create the database"""
    connector = sqlite3.connect('imdbDatabase.db')
    return connector


def constructEntry(row,rowEntry):
    startList = []
    for entryNumber in rowEntry:
        startList.append(row[entryNumber])
    entryTuple = tuple(startList)
    return entryTuple


def constructMovieActorTable(connector, fileName):
    imdbCursor = connector.cursor()
    imdbCursor.execute(("CREATE TABLE " + 'movie_actor(movie_ID, actor_ID, FOREIGN KEY(movie_ID) REFERENCES movie(movieID), FOREIGN KEY(actor_ID) REFERENCES actor(actorID))'))
    with open(fileName) as tsvFile:
        tsvReader = csv.reader(tsvFile, delimiter='\t')
        counter = 0
        for row in tsvReader:
            if counter > 0:
                movies = row[5].split(',')
                for movieIdentifier in movies:
                    values = (movieIdentifier, row[0])
                    rowConstruction = 'INSERT INTO ' + 'movie_actor(movie_ID, actor_ID)' + ' VALUES(?,?)'
                    imdbCursor.execute(rowConstruction,values)
            counter += 1
        connector.commit()


###############################
"""   Schema Explanation   """
###############################
""" The Schema in this database consists of 6 tables. I designed an actors table, a movies table, and an actor-movie table. The actor-movie table is com
comprised of foreign keys (the movie and actor id) stemming from their respective tables so that it could store the fact that there doesn't exist a
one-to-one relationshio from movies to actors in those movoies, at least in general it doesn't. Then I could use join tables between those three to get
the data required by this task. When it comes to user ratings and and comments, I recognize that users are unique but can have munltiple ratings and comments.
Therefore, there is a user table, ratings table, and comments table to differentiate this relationship. It is a 1-Many relationship from user table to
rating and comments table. It is a 1-many relationship from the movie and actor tables to the movie-actor table."""
###############################

def main():
    #Build the connector
    connector = dbConnect()

    #Build tables
    loadTable(connector, 'movie(movieID PRIMARY KEY, movieName)',[0,2],'title.basics.tsv')
    loadTable(connector, 'actor(actorID PRIMARY KEY, actorName)', [0,1],'name.basics.tsv')
    constructMovieActorTable(connector, 'name.basics.tsv')
    #Build tables for comments and ratings
    imdbCursor = connector.cursor()
    imdbCursor.execute('pragma foreign_keys=ON')
    imdbCursor.execute(("CREATE TABLE " + 'user(userID PRIMARY KEY, userName)'))
    imdbCursor.execute(("CREATE TABLE " + 'comment(commentID PRIMARY KEY, movie_ID, user_ID, comment, FOREIGN KEY(movie_ID) REFERENCES movie(movieID), FOREIGN KEY(user_ID) REFERENCES user(userID))'))
    imdbCursor.execute(("CREATE TABLE " + 'rating(ratingID PRIMARY KEY, movie_ID, user_ID, rating, FOREIGN KEY(movie_ID) REFERENCES movie(movieID), FOREIGN KEY(user_ID) REFERENCES user(userID))'))
    connector.commit()


if __name__ == '__main__':
    main()
