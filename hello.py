#! /usr/bin/env python3

import flask
from flask import Flask 
from flask import request 

#initialize the server 
app = flask.Flask(__name__)

#define a /hello route for only post requests


@app.route('/users/<string:username>')
def index(username = None):
	return("Hello {}!".format(username))