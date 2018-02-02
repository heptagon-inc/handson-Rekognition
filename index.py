# coding: UTF-8

#index.py
#sample/にputしたら動く

import os
import re
import json
import urllib
import datetime
import random
import boto3
import logging
import urllib.parse
	
logger = logging.getLogger()
dynamodb = boto3.resource('dynamodb')

#S3オブジェクトを取得
s3 = boto3.resource('s3')

#任意のバケット名に変更する
bucket = 'sort-picture'

#原則サービスのリージョンは揃えるべきだが、clientの引数にリージョン名を指定すれば異なるリージョンでもサービスの連携ができる
#rekognitionとかSESだけを海外リージョンで使うときはこうすれば大丈夫
#また、rekognitionのサービスではs3オブジェクトではなくバイトデータを渡して解析している。
#理由は、s3は東京リージョンで作成したいが、rekognitionは東京リージョンで提供されていないため、
#rekognitionとs3が同リージョンということは現在不可能だから。

rekognition = boto3.client('rekognition', region_name='us-east-1')

def lambda_handler(event, context):

	print('eventの中身:', str(event))

	collection_id = 'face'

	#sample/にputした時の動作

	#S3にアップロードされた画像を取得
	key = event['Records'][0]['s3']['object']['key']
	key = urllib.parse.unquote(key)
	print(key)
	filename = key.split('/')[1].split('.')[0]
	print(filename)
	obj = s3.Object(bucket,key)
	obj_data = obj.get()
	obj_data_body = obj_data['Body'].read()

	#顔が写ってるか判断し、写ってなかったらreturn
	detect = detect_faces(obj_data_body)
	if(len(detect['FaceDetails']) < 1):
		print("顔うつってないぽい")
		return

	#コレクションの検索
	collections = rekognition.list_collections(
		MaxResults=100
	)

	#faceというコレクションがなければ作成
	if(collection_id not in collections['CollectionIds']):
		create_response = rekognition.create_collection(
			CollectionId = collection_id	
		)
		print('create_response_type:' + str(type(create_response)))
		print('create_response:' + str(create_response))

	#画像に写った顔をインデックス
	face_index_response = rekognition.index_faces(
		CollectionId = collection_id,
		Image = {
			'Bytes': obj_data_body
		},
		#ExternalImageId = '画像のidみたいな感じ',
		DetectionAttributes = [
			'ALL',
		]
	)

	print('face_index_response_type:' + str(type(face_index_response)))
	print('face_index_response:' + str(face_index_response))

	#インデックスするまではtemp/にputした時と同じ動作

	#faceIDと人名の関連付けをするために、Dynamoにput
	face_id = face_index_response['FaceRecords'][0]['Face']['FaceId']
	print("インデックスした顔ID:" + str(face_id))

	#全く同じ画像をsample/に置いた時の対応->同じの置かれたらスルー、違うのだったらsample/の画像をDBに登録する処理
	sameid = get_dynamodb("faces", "faceid", face_id)
	if("Item" not in sameid):
		dynamo_name_data = scan_dynamodb('names', '', '')
		last_id = 0
		#Dynamoをscanしたデータが空でないなら
		if(len(dynamo_name_data['Items']) > 0):
			asc_name_list = sorted(dynamo_name_data['Items'], key=lambda x:x['id'])
			#最新のid
			last_id = int(asc_name_list[-1]['id']) + 1
		else:
			last_id = 1

		put_dynamodb("names", {
			'id': last_id,
			'name': filename
		})

		put_dynamodb("faces", {
			'faceid': face_id,
			'nameid': last_id
		})


	#facesテーブルの個人が紐づけられていないデータを全て取得
	dynamo_face_data = scan_dynamodb('faces', 'nameid', 0)
	print('dynamo_face_data:', str(dynamo_face_data))

	#紐づけられていないものがなかったら
	if(len(dynamo_face_data['Items']) < 1):
		return

	for facedata in dynamo_face_data['Items']:
		search_response = rekognition.search_faces(
			CollectionId=collection_id,
			FaceId=facedata['faceid'],
			MaxFaces=100,
			FaceMatchThreshold=80
		)

		print("search_response:" + str(search_response))

		#似ている顔がコレクションになかったときの処理
		if(len(search_response['FaceMatches']) < 1):
			continue

		similar = 0.0
		sim_faceid = ""
		#取得したfaceidといちばん似ている顔をコレクションから探す
		for matches in search_response['FaceMatches']:
			if(similar < matches['Similarity']):
				similar = matches['Similarity']
				sim_faceid = matches['Face']['FaceId']

		print("similar:" + str(similar))
		print("sim_faceid:" + str(sim_faceid))

		if(similar != 0.0 and sim_faceid != ""):
			#コレクションでいちばん似ている判定が出た顔に紐づく個人を、取得したfaceidにも紐づける
			get_face_id = get_dynamodb("faces", "faceid", sim_faceid)
			print("get_face_id:" + str(get_face_id))
			get_name = get_dynamodb("names", "id", get_face_id['Item']['nameid'])
			print("get_name:" + str(get_name))
			update_dynamodb("faces", "faceid", facedata['faceid'], "nameid", get_name['Item']['id'])

			#振り分けが保留になっていた画像を対象の場所に画像をコピー
			#とりあえず、バケット - sorted/ - 人名/ - 日本時間の年月日時分秒.png(または.jpgなど) として保存する
			print("保留されてたkey:" + str(facedata['file']))
			cpObj = s3.Object(bucket,facedata['file'])
			cpObj_data = cpObj.get()
			cpObj_data_body = cpObj_data['Body'].read()

			jstTime = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
			putKey = "sorted/" + get_name['Item']['name'] + "/" + jstTime.strftime('%Y%m%d%H%M%S') + "." + facedata['file'].split('/')[1].split('.')[1]
			putObj = s3.Object(bucket, putKey)#バケット名とパスを指定
			putObj.put(
				ACL='private',#'private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'
				Body=cpObj_data_body
			)#バケットにファイルを出力


	return

def scan_dynamodb(tableName, attr = '', attrVal = ''):
	datatable = dynamodb.Table(tableName)
	scan_data = {}
	if(attr != '' and attrVal != ''):
		scan_data = datatable.scan(
			FilterExpression = boto3.dynamodb.conditions.Attr(attr).eq(attrVal)
		)
	else:
		scan_data = datatable.scan()

	return scan_data

def get_dynamodb(tablename, pkschema, pk):
	table = dynamodb.Table(tablename)
	get_response = table.get_item(
		Key={
			pkschema : pk
		}
	)
	return get_response

def put_dynamodb(tablename, dictData):
	table = dynamodb.Table(tablename)
	put_response = table.put_item(
		Item = dictData
	)

	print('dynamodbにputしたレスポンス' + str(put_response))

	return

def update_dynamodb(tablename, pkschema, pk, attrName, attrVal):
	table = dynamodb.Table(tablename)
	updExp = "SET " + attrName + " = :v"
	update_response = table.update_item(
		Key = {
			pkschema : pk
		},
		UpdateExpression = updExp,
		ExpressionAttributeValues = {":v": attrVal},
	)
	return update_response

def detect_faces(binData):
	detect_response = rekognition.detect_faces(
		Image={
			'Bytes': binData
		},
		Attributes=[
			'ALL',
		]
	)
	return detect_response