from flask import Flask, request
from src.handlers import main_handler
from src.helpers import main_helper

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    main_handler.index()

@app.route('/backup', methods=['GET'])
def backup():
    main_handler.backup()

@app.route('/restore', methods=['GET'])
def restore():
    main_handler.restore()




if __name__ == '__main__':
    app.run(host='localhost', port=8089)
