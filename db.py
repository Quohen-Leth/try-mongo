from bson.objectid import ObjectId

import pymongo

# Database URI, format <db driver "mongodb" or "mongodb+srv">://<username>:<user password>@<database host>:<port>
uri = "mongodb://mongouser:password@localhost:27017"

# Define client.
client = pymongo.MongoClient(uri)

# Define database to connect.
db = client.mongodb

# Define collection.
book_collection = db["books"]


def initial_seed():
    data = [
        {"title": "Title1", "author": "Author1", "published": 2015, "pages": 200},
        {"title": "Title2", "author": "Author1", "published": 2020, "pages": 210},
        {"title": "Title3", "author": "Author2", "published": 2017, "pages": 250},
        {"title": "Title4", "author": "Author2", "published": 2019, "pages": 300},
        {"title": "Title5", "author": "Author3", "published": 2018, "pages": 200},
        {"title": "Title6", "author": "Author3", "published": 2019, "pages": 330},
        {"title": "Title7", "author": "Author3", "published": 2021, "pages": 405},
    ]
    book_collection.insert_many(data)


def read_all_collection():
    return book_collection.find()


def insert_into_collection(title, author, published, pages):
    data = {
        "title": title,
        "author": author,
        "published": int(published),
        "pages": int(pages)
    }
    return book_collection.insert_one(data)


def get_book_by_id(id):
    book = book_collection.find_one({"_id": ObjectId(id)})
    return book


def update_book(id, title, author, published, pages):
    book = {"_id": ObjectId(id)}
    data = {
        "title": title,
        "author": author,
        "published": int(published),
        "pages": int(pages)
    }
    book_collection.update_one(book, {"$set": data})


def search_books(search_string):
    # Method db.collection.find() accepts two arguments, namely "filter" and "projection".
    #
    # "filter" defines conditions, documents should satisfy. For example:
    # {"some_field": some_value} - returns all documents, where field "some_field" exactly equals to some_value. In case when "some_field" is iterable, some_value must be in it.
    # {"some_field": {"$in": [value1, value2]}} - all documents, where "some_field" equals to value1 or value2.
    # 
    # "projection" is like a mask for fields in returned documents.
    # fields of documents to be transfered from database, should be marked as {"some_field": 1}, otherwise they should be marked as {"some_field": 0}. {"_id": 1} by default.
    books = (
        book_collection
        .find(
            {"title": {"$regex": search_string, "$options": "i"}},  # Filter.
            {"_id": 1, "title": 1, "author": 1, "published": 1, "pages": 1}  # Projection.
        )
        .skip(0)
        .limit(5)
        .sort({"title": -1})  # A value of 1 or -1 specifies an ascending or descending sort respectively.
    )
    return books


def author_stats(pipeline):
    """
    This function uses mongodb aggregations to show some statistics on collection documents.
    Aggregations are working through stages, collected into pipelines.
    The order of stages in pipeline's list defines order of data processing.
    """
    unwind_stage = { "$unwind": "$author" }

    match_stage = {"$match": {"author": "Author3"}}

    project_stage = {"$project": {"_id": 0, "author": 1, "pages": 1}}

    group_stage = {
        "$group": {
            "_id": {"author": "$author"},
            "average_pages": {
                "$avg": "$pages"
            },
            "total_books": {
                "$count": {}
            }
        }
    }

    sort_stage = {
        "$sort": {"average_pages": -1}
    }

    bucket_stage = {
        "$facet": {"books_by_number_of_pages": [{
            "$bucket": {
                "groupBy": "$pages",
                "boundaries": [200, 300, 400, 500],
                "default": "other",
                "output": {
                    "count": {"$sum": 1}
                }
            }
        }]}
    }

    # Simple pipeline, works similar to book_collection.find({"author": "Author3"}, {"_id": 0, "author": 1, "pages": 1})
    filter_pipeline = [
        match_stage,
        project_stage
    ]

    # Pipeline to group documents in "books" collection by the  same "author" field,
    # calculate average number of "pages" per author
    # and sort grouped documents by average number of "pages" per author.
    group_pipeline = [
        unwind_stage,
        project_stage,
        group_stage,
        sort_stage,
    ]

    # Categorizes incoming documents into groups, called buckets,
    # based on a specified expression and bucket boundaries and outputs a document per each bucket. 
    bucket_pipeline = [
        bucket_stage,
    ]

    pipeline_selector = {
        "filter": filter_pipeline,
        "group": group_pipeline,
        "bucket": bucket_pipeline
    }

    author_agregation = book_collection.aggregate(pipeline_selector[pipeline])
    return author_agregation
