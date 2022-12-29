#Student Database (MongoDB)

#IMPORT_REQUIRED_MODULES
import pymongo
import json
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://127.0.0.1:27017")

#CREATE_DATABASE_AND_COLLECTION

db = client["STUDENT"]

DATABASE = db["DATABASE"]

#OPEN_AND_LOAD_FILE

with open(r'C:\Users\cprat\PycharmProjects\pythonProject\students.json') as file:
       file_data = [json.loads(i) for i in file]

#INSERT_DATA

if isinstance(file_data, list):
    DATABASE.insert_many(file_data)
else:
    DATABASE.insert_one(file_data)

for i in DATABASE.find():
    print(i)


#1:Student name who scored maximum scores in all (exam, quiz and homework)?

agg = DATABASE.aggregate([
       {"$unwind": "$scores"},
       {"$group":
              {
                     "_id": "$_id",
                     "name": {"$first": "$name"}
                     ,
                     "Total": {"$sum": "$scores.score"},
              }
       },
       {"$sort": {"Total": -1}},
       {"$limit": 1}

]) 


for i in agg:
       print(i)

#2:Students who scored below average in the exam if pass mark is 40%?

agg01 = DATABASE.aggregate([
       {"$unwind": "$scores"},
       {"$match": {'scores.type': 'exam', "scores.score": {"$gt": 40, "$lt": 60}}
        }

])
for i in agg01:
       print(i)

#4:Total and Average of the exam, quiz and homework and store them in a separate collection.

TOTAL_AVG = db.TOTAL_AVG_collection

agg = DATABASE.aggregate([
       {"$unwind": "$scores"},
       {"$group":
              {
                     "_id": "$_id",
                     "name": {"$first": "$name"}
                     ,
                     "Total": {"$sum": "$scores.score"},
                     "Average": {"$avg": "$scores.score"}
              }
       },
       {"$sort": {"_id": 1}}

])

TTAVG = []

for i in agg:
       TTAVG.append(i)
       print(i)

TOTAL_AVG.insert_many(TTAVG)

#5:New collection which consists of students who scored below average and above 40% in all the categories.

AVG_BELOW = db.belowavg_collection

agg = DATABASE.aggregate(
[{"$match":
   {"$expr":
     {"$and":
       [{"$gt": [{"$min": "$scores.score"}, 40]},
         {"$lt": [{"$max": "$scores.score"}, 70]}
        ]
      }
    }
  }])

AVGBEL = []
for i in agg:
  AVGBEL.append(i)
  print(i)
AVG_BELOW.insert_many(AVGBEL)

#6:New collection which consists of students who scored below the fail mark in all the categories.

FAIL = db.FAILED_collection
agg = DATABASE.aggregate(
    [{"$match":
          {"$expr":
               {"$lt": [{"$max": "$scores.score"}, 40]
                }

           }
      }
     ])

FAILED = []

for i in agg:
    FAILED.append(i)
    print(i)

FAIL.insert_many(FAILED)

#7:New collection which consists of students who scored above pass mark in all the categories.

PASS = db.ALLPASS_collection
agg = DATABASE.aggregate(
    [{"$match":
          {"$expr":
               {"$gt": [{"$min": "$scores.score"}, 40]
                }

           }
      }
     ])

PASSED = []

for i in agg:
    PASSED.append(i)
    print(i)
PASS.insert_many(PASSED)