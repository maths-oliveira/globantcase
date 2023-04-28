from flask import request, render_template
from src.helpers import main_helper

def index():
    if request.method == 'GET':
        return render_template('index.html')

    files = request.files.getlist('files')
    file_names = main_helper.upload_files_to_gcs(files)
    main_helper.create_bq_table(file_names)

    return 'Database updated'


def backup():
    main_helper.export_dataset_tables_to_gcs()

    return 'Backup done'

def restore():
    main_helper.import_dataset_tables_from_gcs()

    return 'Restoration done'
