from flask import Flask, request

app = Flask(__name__)


@app.route('/')
def root():
    name = request.args.get('name', 'World')
    return 'adityagovil.cse.msit@gmail.com'
