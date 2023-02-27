import dotenv
import pymongo
from bson import ObjectId
from dotenv import load_dotenv, find_dotenv
from pymongo import MongoClient
import os
import pprint
import dotenv

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")

connection_str = f"mongodb+srv://MichalisKaratsioris:{password}@cluster.wpjmu9f.mongodb.net/?retryWrites=true&w=majority"
client = MongoClient(connection_str)

dbs = client.list_database_names()
# print(dbs)
mongoProject = client.mongoProject
production = client.production
collections_mongoProject = mongoProject.list_collection_names()
collections_production = production.list_collection_names()


# print(collections_mongoProject)
# print(collections_production)


def insert_fromMSSQL_doc():
    collection = mongoProject.fromMSSQL
    mssqlDocument = {
        "name": "Time",
        "type": "Test"
    }
    inserted_id = collection.insert_one(mssqlDocument).inserted_id
    print(inserted_id)


# insert_fromMSSQL_doc()


# when searching for a non-existent db/collection, MongoDB will create it automatically
production = client.production
person_collection = production.person_collection

docs = []


# ########################### CRUD OPERATIONS ###########################


# #################### CREATE


def create_documents():
    first_names = ["Tim", "Sarah", "Jennifer", "Jose", "Brad", "Allen"]
    last_names = ["Ruscica", "Smith", "Bart", "Cater", "Pitt", "Geral"]
    ages = [21, 40, 23, 19, 34, 67]
    for first_name, last_name, age in zip(first_names, last_names, ages):
        document = {"first_name": first_name, "last_name": last_name, "age": age}
        docs.append(document)
        # person_collection.insert_one(docs)
    person_collection.insert_many(docs)


# create_documents()

# ------------------------------------------------------------------------
# ------------------------------------------------------------------------

#################### READ


printer = pprint.PrettyPrinter()


def find_all_people():
    people = person_collection.find({})
    # print(people)
    # print(list(people))
    for person in people:
        printer.pprint(person)


# find_all_people()


def get_person_by_first_name():
    first_name = person_collection.find_one({"first_name": "Tim"})
    printer.pprint(first_name)


#     If we have used .find({"first_name": "Tim"}), then inside the printer I should have used list(first_name)
#     instead of just first_name.


# get_person_by_first_name()


def count_all_people():
    # count = person_collection.find().count()
    count = person_collection.count_documents(filter={})
    print("Number of people in the collection: ", count)


# count_all_people()


def get_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id": _id})
    printer.pprint(person)


# get_person_by_id("637281c4a99c32e743a4d84b")


def get_age_range(min_age, max_age):
    query = {"$and": [
        {"age": {"$gte": min_age}},
        {"age": {"$lte": max_age}}
    ]}
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)


# get_age_range(20, 35)


def project_columns():
    columns = {"_id": 0, "first_name": 1, "last_name": 1}
    people = person_collection.find({}, columns)
    for person in people:
        printer.pprint(person)


# project_columns()
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------


#################### UPDATE


def update_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    # add data
    # all_updates = {
    #     "$set": {"new_field": True},
    #     "$inc": {"age": 1},
    #     "$rename": {"first_name": "firstName", "last_name": "lastName"}
    # }
    # person_collection.update_one({"_id": _id}, all_updates)

    # delete data
    # person_collection.update_one({"_id": _id}, {"$unset": {"new_field": ""}})


# update_person_by_id("637281c4a99c32e743a4d84b")


def replace_person_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    new_document = {
        "first_name": "new first name",
        "last_name": "new last name",
        "age": 100
    }
    person_collection.replace_one({"_id": _id}, new_document)


# replace_person_by_id("637281c4a99c32e743a4d84b")

new_address = {
    "_id": "62475964011a9126a4cebeb7",
    "street": "Bay Street",
    "number": 2706,
    "city": "San Francisco",
    "country": "United States",
    "zip": "94107"
}


# creating new field in persons from another collection, i.e. address
def add_address_embed(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person_collection.update_one({"_id": _id}, {"$addToSet": {"addresses": address}})


# add_address_embed("637281c4a99c32e743a4d84f", new_address)


# creating relations between collections: persons & address
def add_address_relationship(person_id, address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)


# add_address_relationship("637281c4a99c32e743a4d84e", new_address)
# ------------------------------------------------------------------------
# ------------------------------------------------------------------------


#################### DELETE


def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    # delete every document
    # person_collection.delete_many({})

    # delete one document
    # person_collection.delete_one({"_id": _id})


# delete_doc_by_id("637281c4a99c32e743a4d84b")


def delete_docs_by_id():
    # delete a group of documents
    query = {"_id": {"$in": [ObjectId("637281c4a99c32e743a4d84a"), ObjectId("637281c4a99c32e743a4d84c")]}}
    # id1 = {"_id": "637281c4a99c32e743a4d84a"}
    # id2 = {"_id": "637281c4a99c32e743a4d84c"}
    # person_collection.delete_many(id1, id2)
    result = person_collection.delete_many(query)


delete_docs_by_id()

