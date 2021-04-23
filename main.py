from flask import Flask, render_template, request, make_response
from services import dynamo_service as ds
from services import s3_service as s3s

app = Flask(__name__)


@app.route("/")
def home():
    # TODO: check if table exists in main.
    #ds.create_music_table()
    #ds.load_music()
    upload_all_images()
    return render_template('home.html')


def upload_all_images():
    urls = ds.get_all_img_urls()
    print(urls)


@app.route("/login", methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        email = request.form['email']
        pwd = request.form['password']
        if valid_login(email, pwd):
            print("Authorization successful.")
            # route to main area
        else:
            print("Authorization Unsuccessful.")
            error = "email or password is invalid"

    return render_template('login.html', message=error)


def valid_login(email, password):
    login = ds.get_login(email)
    if login is not None:
        return login['password'] == password
    return False


@app.route("/register", methods=['GET', 'POST'])
def register():
    return render_template('register.html', message="")


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
