from flask import Flask, request
from flask_restful import Resource, Api
from sqlalchemy import create_engine
from flask import render_template
from flask_sqlalchemy import SQLAlchemy
import time
from math import ceil

appEngine = create_engine('sqlite:///imdbDatabase.db')

app = Flask(__name__)
api = Api(app)
db = SQLAlchemy(app)

###################################################################
'''Search for names of actors in a given movie'''
###################################################################
@app.route('/actors', methods=['GET', 'POST'])
def findActors():
    '''This function presents the user with a field to fill out when presented with GET or takes the POST message
    and parses out the user input and returns a json output of the table'''
    if request.method == 'POST':
        connector = appEngine.connect()
        movieName = request.form['movieName']
        actorQuery = connector.execute("SELECT actor.actorName, movie.movieName FROM actor INNER JOIN movie_actor ON actor.actorID=movie_actor.actor_ID INNER JOIN movie ON movie_actor.movie_ID=movie.movieID WHERE movie.movieName=(?)", movieName)
        result = {'data': [dict(zip(tuple(actorQuery.keys()), i)) for i in actorQuery.cursor]}
        return result
    return render_template('actors.html')


#####################################################
'''Search for names of actors in a given movie'''
#####################################################
@app.route('/movies', methods=['GET', 'POST'])
def findMovies():
    '''This function presents the user with a field to fill out when presented with GET or takes the POST message
        and parses out the user input and returns a json output of the table'''
    if request.method == 'POST':
        connector = appEngine.connect()
        actorName = request.form['actorName']
        actorQuery = connector.execute("SELECT actor.actorName, movie.movieName FROM actor INNER JOIN movie_actor ON actor.actorID=movie_actor.actor_ID INNER JOIN movie ON movie_actor.movie_ID=movie.movieID WHERE actor.actorName=(?)", actorName)
        result = {'data': [dict(zip(tuple(actorQuery.keys()), i)) for i in actorQuery.cursor]}
        return result
    return render_template('movies.html')


#################################################
'''Rate a movie on a 1-5 scale, with a comment'''
#################################################
@app.route('/rating_enter', methods=['GET', 'POST'])
def enterRating():
    """This function begins by presenting the user with a form. Once the form is submitted, helper functions
    do work to upload the user data to the database."""
    if request.method == 'POST':
        movieName = request.form['movieName']
        username = request.form['userName']
        rating = request.form['rating']
        comment = request.form['comment']
        post([movieName, username, rating, comment])
    return render_template('rating_enter.html')

def post(userInput):
    '''This helper function actually loads the data into the database'''
    movieName = userInput[0]
    userName = userInput[1]
    rating = int(userInput[2])
    comment = str(userInput[3])
    comment = comment.lstrip("(u'")
    comment = comment.rstrip("',)")
    connector = appEngine.connect()
    idStatus = findUniqueUserID(userName)
    if idStatus == False:
        addUserEntry(userName)
    userIdentifier = str(findUser(userName))
    userIdentifier = userIdentifier.lstrip("(u'")
    userIdentifier = userIdentifier.rstrip("',)")
    ratingID = createUniqueRatingId()
    commentID = createUniqueRatingId()
    movieIdentifier = str(connector.execute("SELECT movie.movieID FROM movie WHERE movie.movieName=(?)", movieName).fetchone())
    movieIdentifier = movieIdentifier.lstrip("(u'")
    movieIdentifier = movieIdentifier.rstrip("',)")
    connector.execute('INSERT INTO rating(ratingID, movie_ID, user_ID, rating) VALUES(?,?,?,?)', (ratingID,movieIdentifier,userIdentifier,rating))
    connector.execute('INSERT INTO comment(commentID, movie_ID, user_ID, comment) VALUES(?,?,?,?)', (commentID, movieIdentifier, userIdentifier, comment))


def findUniqueUserID(userID):
    """This functions takes a user's usernamae and determines wether the user's username has been registered in the
    database yet"""
    connector = appEngine.connect()
    userIdentifier = connector.execute("SELECT user.userID FROM user WHERE userName=(?)", userID).fetchone()
    #userIdentifier = db.session.execute("SELECT user.userID FROM user WHERE userName=(?)", userID)
    if type(userIdentifier) == type(None):
        return False # this means there is no user in the database yet
    else:
        return True # this means there is a user in the database

def findUser(username):
    """This function finds a userid from the username"""
    connector = appEngine.connect()
    userId = connector.execute("SELECT user.userID FROM user WHERE userName=(?)", username).fetchone()
    #selectInput = select([user]).where(user.column.userName == username)
    #db.execute(selectInput)
    return userId

def addUserEntry(userName):
    """This function adds users to the database the first time they add a rating/comment"""
    connector = appEngine.connect()
    rows = connector.execute('SELECT count(*) FROM user').rowcount
    newUserId = 'u' + str(ceil(time.time()))
    connector.execute('INSERT INTO user(userID,userName) VALUES(?, ?)', (newUserId, userName))

def createUniqueRatingId():
    """This function creates a unique id"""
    #connector = appEngine.connect()
    ratingID = 'r' + str(ceil(time.time()))
    return ratingID

####################################################
'''Search movies with a rating above a set value, and return both the movie name and the actors in it'''
#####################################################
@app.route('/rating_search', methods=['GET', 'POST'])
def findRatings():
    """This function obtains a rating from the user and then carries out an inner join over 4 tables to accomplish the
    task"""
    if request.method == 'POST':
        connector = appEngine.connect()
        rating = int(request.form['rating'])
        joinTable = connector.execute("SELECT movie.movieName, actor.actorName, rating.rating FROM movie INNER JOIN rating ON movie.movieID=rating.movie_ID INNER JOIN movie_actor ON movie.movieID=movie_actor.movie_ID INNER JOIN actor ON movie_actor.actor_ID=actor.actorID WHERE rating.rating >= (?);", (rating))
        result = {'data': [dict(zip(tuple(joinTable.keys()), i)) for i in joinTable.cursor]}
        return result
    return render_template('rating_search.html')


if __name__ == '__main__':
    app.run(port='5002')