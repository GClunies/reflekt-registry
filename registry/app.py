from datetime import datetime

from chalice import Chalice

app = Chalice(app_name="registry")


@app.route("/")
def index():
    """Return a simple hello world JSON response.

    Returns:
        dict: A simple JSON response.
    """
    return {"hello": "world"}


@app.route("/today")
def today():
    """Return today's date.

    Returns:
        str: Today's date.
    """
    return datetime.today().strftime("%Y-%m-%d")


# The view function above will return {"hello": "world"}
# whenever you make an HTTP GET request to '/'.
#
# Here are a few more examples:
#
# @app.route('/hello/{name}')
# def hello_name(name):
#    # '/hello/james' -> {"hello": "james"}
#    return {'hello': name}
#
# @app.route('/users', methods=['POST'])
# def create_user():
#     # This is the JSON body the user sent in their POST request.
#     user_as_json = app.current_request.json_body
#     # We'll echo the json body back to the user in a 'user' key.
#     return {'user': user_as_json}
#
# See the README documentation for more examples.
#
