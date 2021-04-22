from flask import Flask, render_template, request, make_response
import dynamo_service as ds
from datetime import datetime
from werkzeug.utils import redirect

app = Flask(__name__)


@app.route("/")
def home():
    ds.get_all_logins()
    return render_template('home.html')


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
