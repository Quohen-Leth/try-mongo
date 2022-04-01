from crypt import methods
from flask import Flask, render_template, request

import db

app = Flask(__name__)


@app.route("/", methods=["GET", "POST"])
def view_books():
    objects = None
    if request.method == "POST":
        objects = db.search_books(request.form["search"])
    if request.method == "GET":
        objects = db.read_all_collection()
    return render_template("list.html", objects=objects)


@app.route("/add", methods=["GET", "POST"])
def add_book():
    obj = None
    if request.method == "POST":
        title = request.form["title"]
        author = request.form["author"]
        published = request.form["published"]
        pages = request.form["pages"]
        obj = db.insert_into_collection(title, author, published, pages)
    return render_template("create.html", obj=obj)


@app.route("/<id>", methods=["GET", "POST"])
def book_details(id):
    obj = None
    if request.method == "GET":
        obj = db.get_book_by_id(id)
    if request.method == "POST":
        db.update_book(
            id,
            request.form["title"],
            request.form["author"],
            request.form["published"],
            request.form["pages"]
        )
        obj = db.get_book_by_id(id)
    return render_template("detail.html", obj=obj)


@app.route("/stats/<any('filter', 'group', 'bucket'):pipeline>")
def books_statistics(pipeline):
    author_statistics = db.author_stats(pipeline)
    return {"author_statistics": list(author_statistics)}


# @app.before_first_request
# def first_run():
#     # Run once to seed (fill) database with some data.
#     db.initial_seed()


if __name__ == "__main__":
    app.run(debug=True)
