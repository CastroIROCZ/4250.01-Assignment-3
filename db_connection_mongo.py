#-------------------------------------------------------------------------
# AUTHOR: Eduardo Castro Becerra
# FILENAME: db_connection_mongo
# SPECIFICATION: Script that handles the creation of documents as well as the addition, deletion, and displaying of document contents
# FOR: CS 4250- Assignment #3
# TIME SPENT: About 2 days, 5 hours, and 20 minutes
#-----------------------------------------------------------*/

#IMPORTANT NOTE: DO NOT USE ANY ADVANCED PYTHON LIBRARY TO COMPLETE THIS CODE SUCH AS numpy OR pandas. You have to work here only with
# standard arrays

#importing some Python libraries
# --> add your Python code here
from pymongo import MongoClient
import datetime

def connectDataBase():
    # Create a database connection object using pymongo
    # --> add your Python code here
    client = MongoClient('localhost', 27017)

    db = client.Assignment3
    return db

def createDocument(col, docId, docText, docTitle, docDate, docCat):

    # create a dictionary indexed by term to count how many times each term appears in the document.
    # Use space " " as the delimiter character for terms and remember to lowercase them.
    # --> add your Python code here
    terms = {}
    for term in docText.lower().split():
        if term not in terms:
            terms[term] = {'count': 0, 'num_chars': len(terms)}
        terms[term]['count'] += 1

    # create a list of objects to include full term objects. [{"term", count, num_char}]
    # --> add your Python code here
    term_objects = [{'term': k, 'term_count': v['count'], 'num_chars': v['num_chars']} for k, v in terms.items()]

    # produce a final document as a dictionary including all the required document fields
    # --> add your Python code here
    document = {
        "_id": docId,
        "text": docText,
        "title": docTitle,
        "num_chars": sum(len(word) for word in docText if word.isalnum()),
        "date": datetime.datetime.strptime(docDate, '%Y-%m-%d'),
        "category": docCat,
        "terms": term_objects
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
        for term_info in document["terms"]:
            term = term_info["term"]
            if term not in index:
                index[term] = []
            index[term].append(f"{document['title']}:{term_info['term_count']}")
    return index