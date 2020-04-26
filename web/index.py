from json import JSONEncoder
from flask import Flask, jsonify, request, send_from_directory
import os
import mysql.connector as mysql
import mysql.connector.errors as mysql_errors
from helpers.Config import Config
from datetime import date


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.strftime('%Y-%m-%d %H:%M:%S')
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


app = Flask(__name__, static_folder='static')
app.json_encoder = CustomJSONEncoder

# get Config
config = Config("../config/config.yaml").get()

# Connect to DB
try:
    db = mysql.connect(
        host=config['database']['host'],
        port=config['database']['port'],
        user=config['database']['user'],
        passwd=config['database']['password'],
        database=config['database']['database']
    )

    cursor = db.cursor(dictionary=True)
except (mysql_errors.ProgrammingError, mysql_errors.InterfaceError) as exc:
    if config['general']['debug']:
        msg = 'Cannot connect to database. Got error: {}'.format(exc)
    else:
        msg = 'Cannot connect to database.'

    raise Exception(msg)


@app.route("/")
@app.route("/index")
def index():
    return 'Hi this is an API!'


@app.route('/favicon.ico')
def favicon():
    return send_from_directory(
        os.path.join(app.root_path, 'static'),
        'favicon.ico',
        mimetype='image/vnd.microsoft.icon'
    )


@app.route("/api/general")
def api():
    cursor.execute('SELECT * FROM `main` ORDER BY `id` DESC LIMIT 1')
    general = cursor.fetchone()

    if general is None:
        return jsonify()

    data = {
        'confirmed': general['confirmed_cases'],
        'recovered': general['recovered_cases'],
        'suspected': general['suspected_cases'],
        'deaths': general['deaths'],
        'monitored': general['monitored_cases'],
        'last_update': general['updated']
    }

    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=config['general']['debug'])
