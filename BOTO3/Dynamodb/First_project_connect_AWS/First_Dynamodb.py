import os, sys, boto3, datetime


db = boto3.resource("dynamodb")
db.Table("alpha")