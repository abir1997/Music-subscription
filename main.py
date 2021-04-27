from flask import Flask, render_template, request, make_response, redirect
from services import dynamo_service as ds
from services import s3_service as s3s

app = Flask(__name__)


@app.route("/")
def home():
    # TODO: check if table exists in main.
    #ds.create_music_table()
    #ds.load_music()
    #upload_all_images()
    return render_template('home.html')


def upload_all_images():
    urls = ds.get_all_img_urls()
    s3s.upload_from_urls(urls)


@app.route("/login", methods=['GET', 'POST'])
def login():
    error = None
    if request.method == 'POST':
        email = request.form['email']
        pwd = request.form['password']
        if valid_login(email, pwd):
            print("Authorization successful.")
            logged_in_user = ds.get_login(email)
            resp = make_response(redirect("/mainpage"))
            resp.set_cookie('user_name', logged_in_user['user_name'])
            resp.set_cookie('email', logged_in_user['email'])
            return resp
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
    error = None
    if request.method == 'POST':
        email = request.form['email']
        pwd = request.form['password']
        user_name = request.form['userName']

        if email_exists(email):
            error = "The email already exists"
        else:
            ds.put_login(email, pwd, user_name)
            return make_response(redirect("/login"))

    return render_template('register.html', message=error)


def email_exists(email):
    login_acc = ds.get_login(email)
    return login_acc is not None


@app.route("/mainpage", methods=['GET', 'POST'])
def mainpage():
    subscriptions = ds.get_all_subscriptions(request.cookies.get("email"))
    if request.method == 'POST':
        error = None
        # Get filters
        title = request.form.get('title')
        year = request.form.get('year')
        artist = request.form.get('artist')

        subscription_options = ds.get_music(artist, title, year)

        if not subscription_options:
            error = "No result is retrieved. Please query again"

        sub = request.form.get('subscription')
        if sub:
            ds.put_subscription(request.cookies.get("email"), sub)
            subscriptions = ds.get_all_subscriptions(request.cookies.get("email"))
            error = None

        remove = request.form.get('remove')
        if remove:
            ds.remove_subscription(request.cookies.get("email"), sub)
            subscriptions = ds.get_all_subscriptions(request.cookies.get("email"))
            error = None

        return render_template('mainpage.html', username=request.cookies.get("user_name"),
                               subscriptions=subscriptions, music_list=subscription_options, message=error)

    return render_template('mainpage.html', username=request.cookies.get("user_name"), subscriptions=subscriptions)


if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)
