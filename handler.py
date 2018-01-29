import json
from newsapi import NewsApiClient
import boto3
import datetime
from boto3.dynamodb.conditions import Key, Attr
import pymysql.cursors
import pymysql
import random
import math

def datapull(event, context):
    pageSize = 100
    pageNum = 1

    def getCredentials():
        dynamoDBtemp = boto3.resource('dynamodb', region_name='us-east-1')
        credentialsTable = dynamoDBtemp.Table('ApplicationCredentials')
        response = credentialsTable.query(
            KeyConditionExpression=Key('key').eq('NewsPipeline')
        )
        print("Credentials Retrieved!")
        return response['Items'][0]

    def checkForRecords(cursor, connection):
        cursor.execute("SELECT COUNT(*) FROM news_sources WHERE language = 'en' AND pullflag = 1")
        count = cursor.fetchone()[0]
        if count == 0:
            cursor.execute("UPDATE news_sources SET pullflag = 1 WHERE language = 'en'")
            connection.commit()

    def retrieveNewsSource(credentials):
        connection = pymysql.connect( host=credentials['SourcesDBHost'], user=credentials['SourcesDBUser'], passwd=credentials['SourcesDBPassword'], db=credentials['SourcesDB'] )
        cursor = connection.cursor()
        checkForRecords(cursor, connection)
        cursor.execute("SELECT * FROM news_sources WHERE language = 'en' AND pullflag = 1")
        data = cursor.fetchone()
        connection.close()
        print("Source Retrieved:")
        print(data)
        return data

    def getCallNumber(totalResults, pageSize):
        trueTotal = int(math.ceil(totalResults/pageSize))
        if trueTotal <= 100:
            return trueTotal
        return 100

    def updateSourceDB(source_id, credentials, timepulled=None):
        if timepulled == None:
            updateStatement = "UPDATE news_sources SET pullflag = 0 WHERE id = '%s'" % (source_id)
        else:
            updateStatement = "UPDATE news_sources SET pullflag = 0, lastdatetimepulled = '%s' WHERE id = '%s'" % (timepulled, source_id)
        connection = pymysql.connect( host=credentials['SourcesDBHost'], user=credentials['SourcesDBUser'], passwd=credentials['SourcesDBPassword'], db=credentials['SourcesDB']  )
        cursor = connection.cursor()
        cursor.execute(updateStatement)
        connection.commit()
        connection.close()

    def writeArticlesToDB(table, articles):
        for article in articles:
            for i in article:
                if article[i] == '':
                    article[i] = 'Null'
            articleID = str(random.randint(1, 201)) + "_" + article['source']['id']
            try:
                response = table.put_item(
                    Item={
                        'ArticleID': articleID,
                        'author': article['author'],
                        'description': article['description'],
                        'publishedAt': article['publishedAt'],
                        'source': article['source'],
                        'title': article['title'],
                        'url': article['url'],
                        'urlToImage': article['urlToImage'],
                        'writtenAt':  str(datetime.datetime.now())
                    }
                )
            except:
                print("there was a problem with this record:")
                print(article)

    credentials = getCredentials()
    newsapi = NewsApiClient(api_key=credentials['NewsAPIKey'])
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('News-Articles')
    source = retrieveNewsSource(credentials)

    firstCallArticles = newsapi.get_everything(sources=source[0], from_parameter=source[8], sort_by='publishedAt', page=pageNum, page_size=pageSize)
    print("Articles Retrieved: %d" % firstCallArticles['totalResults'])
    writeArticlesToDB(table, firstCallArticles['articles'])


    if firstCallArticles['totalResults'] > pageSize:
        numOfCalls = getCallNumber(firstCallArticles['totalResults'], pageSize)
        pageNum += 1
        while pageNum <= numOfCalls:
            articles = newsapi.get_everything(sources=source[0], from_parameter=source[8], sort_by='publishedAt', page=pageNum, page_size=pageSize)
            writeArticlesToDB(table, articles['articles'])
            pageNum +=1

    if firstCallArticles['totalResults'] == 0:
        updateSourceDB(source_id = source[0], credentials = credentials)
    else:
        updateSourceDB(source_id=source[0], credentials=credentials, timepulled=firstCallArticles['articles'][0]['publishedAt'])

if __name__ == "__main__":
    datapull('', '')

