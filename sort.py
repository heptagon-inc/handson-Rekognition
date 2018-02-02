# coding: UTF-8

#sort.py
#temp/にアップされた時に動く

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

	#S3にアップロードされた画像を取得
	key = event['Records'][0]['s3']['object']['key']
	key = urllib.parse.unquote(key)
	print(type(key))
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

	#faceってコレクションがなければ作成
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
		#ExternalImageId = "aaa",
		DetectionAttributes = [
			'ALL',
		]
	)

	print('face_index_response_type:' + str(type(face_index_response)))
	print('face_index_response:' + str(face_index_response))

	#振り分け作業
	for facedata in face_index_response['FaceRecords']:
		search_response = rekognition.search_faces(
			CollectionId=collection_id,
			FaceId=facedata['Face']['FaceId'],
			MaxFaces=100,
			FaceMatchThreshold=80
		)

		print("search_response:" + str(search_response))

		#似ている顔がコレクションになかった かつ DBに同じ顔情報がすでに登録されていない時 の処理
		if(len(search_response['FaceMatches']) < 1):
			print("現在のコレクションには似ている顔が見つからなかったです")
			dict_data = {
				'faceid': facedata['Face']['FaceId'],
				'nameid': 0,
				'file': key
			}
			put_dynamodb("faces", dict_data)
			continue


		similar = 0.0
		sim_faceid = ""
		#振り分けて欲しい写真の顔で、いちばん似ている顔をコレクションから探す
		for matches in search_response['FaceMatches']:
			if(similar < matches['Similarity']):
				similar = matches['Similarity']
				sim_faceid = matches['Face']['FaceId']

		print("similar:" + str(similar))
		print("sim_faceid:" + str(sim_faceid))

		if(similar != 0.0 and sim_faceid != ""):
			#コレクションでいちばん似ている判定が出た顔に紐づく個人を、今回インデックスした顔にも紐づける
			get_face_id = get_dynamodb("faces", "faceid", sim_faceid)
			get_name = get_dynamodb("names", "id", get_face_id['Item']['nameid'])
			dict_data = {
				'faceid': facedata['Face']['FaceId'],
				'nameid': get_name['Item']['id']
			}
			put_dynamodb("faces", dict_data)

			#対象の場所に画像をコピー
			#とりあえず、バケット - sorted/ - 人名/ - 日本時間の年月日時分秒.png(または.jpgなど) として保存する
			jstTime = datetime.datetime.utcnow() + datetime.timedelta(hours=9)
			putKey = "sorted/" + get_name['Item']['name'] + "/" + jstTime.strftime('%Y%m%d%H%M%S') + "." + key.split('/')[1].split('.')[1]
			putObj = s3.Object(bucket, putKey)#バケット名とパスを指定
			putObj.put(
				ACL='private',#'private'|'public-read'|'public-read-write'|'authenticated-read'|'aws-exec-read'
				Body=obj_data_body
			)#バケットにファイルを出力

	return

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