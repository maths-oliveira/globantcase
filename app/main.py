from flask import Flask, request
from src.handlers import main_handler
from src.helpers import main_helper

app = Flask(__name__)

@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'GET':
        return main_handler.index()

    files = request.files.getlist('files')
    file_names = main_helper.upload_files_to_gcs(files)
    main_helper.create_bq_table(file_names)

    return 'Database updated'




if __name__ == '__main__':
    app.run(host='localhost', port=8089)
