#-------------------------------------------------------------------------
# AUTHOR: Mark Haddad
# FILENAME: db_connection_mongo.py
# SPECIFICATION: MongoDB Python program allowing document creation, deletion, updating, and output of the document index
# FOR: CS 4250- Assignment #3
# TIME SPENT: 1 day, 5 hours
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import string

def connectDataBase():

    # Create a database connection object using pymongo
    # --> add your Python code here
    DB_NAME = "assignment3"
    DB_HOST = "localhost"
    DB_PORT = 27017

    try:
        client = MongoClient(host=DB_HOST, port=DB_PORT)
        db = client[DB_NAME]

        return db
    except:
        print("Database not connected successfully")

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    term_dict = {}
    docText = remove_capitals_and_punctuation(docText)

    terms = docText.split(" ")

    for term in terms:
        if term_dict.get(term) is None:
            term_dict[term] = 1
        else:
            term_dict[term] += 1


    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term_list = []
    total_num_chars = 0

    for term in terms:
        total_num_chars += len(term)
        term_obj = {
            "term": term, 
            "num_chars": len(term), 
            "term_count": term_dict[term]
            }
        if term_obj not in term_list:
            term_list.append(term_obj)

    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": total_num_chars,
        "date": docDate,
        "category": docCat,
        "terms": term_list
    }

    # insert the document
    # --> add your Python code here
    col.insert_one(document)

def deleteDocument(col, docId):

    # Delete the document from the database
    # --> add your Python code here
    col.delete_one({"_id": docId})

def updateDocument(col, docId, docText, docTitle, docDate, docCat):

    # Delete the document
    # --> add your Python code here
    deleteDocument(col, docId)

    # Create the document with the same id
    # --> add your Python code here
    createDocument(col, docId, docText, docTitle, docDate, docCat)

def getIndex(col):

    # Query the database to return the documents where each term occurs with their corresponding count. Output example:
    # {'baseball':'Exercise:1','summer':'Exercise:1,California:1,Arizona:1','months':'Exercise:1,Discovery:3'}
    # ...
    # --> add your Python code here
    index = {}
    for document in col.find():
        for term_obj in document["terms"]:
            term = term_obj["term"]
            if term not in index:
                index[term] = []
            index[term].append(f"{document['title']}:{term_obj['term_count']}")
    return index

def remove_capitals_and_punctuation(text):
  text = text.lower()
  table = str.maketrans('', '', string.punctuation)
  return text.translate(table)