from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext
from operator import add
import json, sys
from pyspark.sql import SQLContext 
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql.types import * 
from pyspark_cassandra import CassandraSparkContext 

from artistData import artistData
#from albumData import albumData
#from trackData import trackData

artistId = 0
albumId  = 0
trackId  = 0

def fix_wiki_json(x):
	gs = x
	start_sum = gs.find("'summary':") + 12
	end_sum = gs.find(", 'content':") - 1

	
	try:	
		if gs[start_sum - 1] == "\\":
			gs = gs[:start_sum-2] + " " + gs[start_sum:]
			start_sum = gs.find("'summary':") + 12
			end_sum = gs.find(", 'content':") - 1
	except:
		pass

	new_sum = gs[start_sum: end_sum].replace('\\\\"', '"')
	new_sum = new_sum.replace("\\\\'", "'")
	new_sum = new_sum.replace('\\"', '"')
	new_sum = new_sum.replace("\\'", "'")
	new_sum = new_sum.replace('"', '')	
	new_sum = new_sum.replace("'", '')	
	#new_sum = new_sum.replace("\\", '')	

	gs = gs[:start_sum] + new_sum + gs[end_sum:]	
	
	start_con = gs.find("'content':") + 12
	end_con = gs.find(", 'followers':") - 1
	
	try:
		if gs[start_con - 1] == "\\":
			gs = gs[:start_con-2] + " " + gs[start_con:]
			start_con = gs.find("'content':") + 12
			end_con = gs.find(", 'followers':") - 1
	except:
		pass

	new_con = gs[start_con: end_con].replace('\\\\"', '')
	new_con = new_con.replace("\\\\'", "'")
	new_con = new_con.replace('\\"', '"')
	new_con = new_con.replace("\\'", "'")	
	new_con = new_con.replace("'", '')	
	new_con = new_con.replace('"', '')	
	#new_con = new_con.replace('\\', '')

        gs = gs[:start_con] + new_con + gs[end_con:]
	#print gs	
	return gs

def correct_json(x):
	x = x.replace("u'", "'")
	x = x.replace('u"', '"')
	gs = x
        gs = json.dumps(x)

	gs = fix_wiki_json(gs)

	#changes first quote, ex. , 'content': -> , "content':
	#changes second quote, ex. "content': -> "content":
	gs = gs.replace(", '", ', "')
	gs = gs.replace("': ", '": ')
	
	#changes first quote, ex. "content": 'Weezer...' -> "content": "Weezer...'
	#changes second quote, ex. "content": "Weezer...' -> "content": "Weezer..."
	gs = gs.replace(": '", ': "')
	gs = gs.replace("', ", '", ')
	
	#changes first quote, ex. {'Weezer'... '} -> {"Weezer'... '}
	#changes second quote, ex. {"Weezer'... '} -> {"Weezer'... "}
	gs = gs.replace("{'", '{"')
	gs = gs.replace("'}", '"}')
	
	#changes first quote, ex. ['alternative metal'] -> ["alternative metal']
	#changes second quote, ex. "'alternative metal'] -> ["alternative metal"]
	gs = gs.replace("['", '["')
	gs = gs.replace("']", '"]')
	
	#for cases like "name": \"Hello\" -> "name": \"Hello\"
	gs = gs.replace('": \\"', '": "')
	gs = gs.replace('\\", "', '", "')
	
	gs = gs.replace('\\\\"', '\\"')
	gs = gs.replace("\\\\'", "\\'")

	gs = gs.replace(', \\"', ', "')
	gs = gs.replace(", \\'", ", '")


	#removes quotes around the beginning and end
	gs = gs[1:len(gs)-1]
	#print gs
	return json.loads(json.dumps(gs))
	

def get_most_common_word(x):
	words = {}
	stop_words = ['a', 'about', 'above', 'after', 'again', 'against', 'all', 'am', 'an', 'and', 'any', 'are', 'aren\'t', 'as', 'at', 'be', \
			'because', 'been', 'before', 'being', 'below', 'between', 'both', 'but', 'by', 'cant', 'cannot', 'could', 'couldnt', \
			'did', 'didnt', 'do', 'does', 'doesnt', 'doing', 'dont', 'down', 'during', 'each', 'few', 'for', 'from', 'further', \
			'had', 'hadnt', 'has', 'hasnt', 'have', 'havent', 'having', 'he', 'hed', 'hell', 'hes', 'her', 'here', 'herself', 'him', \
			'himself', 'his', 'himself', 'his', 'how', 'hows', 'i', 'id', 'ill', 'im', 'ive', 'if', 'in', 'into', 'is', 'isnt', 'it', \
			'its', 'itself', 'lets', 'me', 'more', 'most', 'mustnt', 'my', 'myself', 'no', 'nor', 'not', 'of', 'off', 'on','once', \
			'only', 'only', 'or', 'other', 'ought', 'our', 'ours', 'ourselves', 'out', 'over', 'own', 'same', 'shant', 'she', 'shed', \
			'shell', 'shes', 'should', 'shouldnt', 'so', 'some', 'such', 'than', 'that', 'thats', 'the', 'their', 'theirs', 'them', \
			'themselves', 'then', 'there', 'theres', 'these', 'they', 'theyd', 'theyll', 'theyre', 'theyve', 'this', 'those', 'through', \
			'to', 'too', 'under', 'until', 'up', 'very', 'was', 'wasnt', 'we', 'wed', 'well', 'were', 'weve','werent', 'what', 'whats', \
			'when', 'whens', 'where', 'wheres', 'which', 'while', 'who', 'whos', 'whom', 'why', 'whys', 'with', 'wont', 'would', 'wouldnt', \
			'you', 'youd', 'youll', 'youre', 'youve', 'your', 'yours', 'yourself', 'yourselves']
	for i in x.split(" "):
		if i.lower() in stop_words:
			continue
		if i not in words:
			words[i] = 1
		else:
			words[i] += 1
	max_cnt = 0
	max_word = ""
	for k,v in words.iteritems():
		if v > max_cnt:
			max_cnt = v
			max_word = k
	return max_word

def get_artist_stats(x):
	json_data = x.asDict()
		
	if len(json_data) <= 0:
		return
	try:
		# getartist name like js['name']
		#and the rest dont need to ref the name first i think
		artist_name = json_data['name']
		artist_pop = json_data['popularity']
		artist_sum = json_data['summary']
		artist_content = json_data['content']
		artist_followers = json_data['followers']
		artist_genres = json_data['genres']
		artist_album_num = 0
		artist_song_num = 0 
		artist_duration = 0
		artist_albums = []
		album_avg_song = []
		album_duration = []
		album_popularity = []
		album_release = []
		album_tracks = []
		artist_track = []
		track_popularity = []
		track_duration = []
		for i in json_data['albums']:
			if i['name'].lower() in artist_albums:
				continue
			artist_album_num += 1
			artist_albums.append(i['name'].lower())
			album_popularity.append(i['popularity'])
			album_release.append(i['release_date'])
			temp_duration = 0
			temp_tracks = 0
			for j in i['tracks']:
				artist_track.append(j['name'].lower())
				try:
					track_popularity.append(j['popularity'])
				except:
					track_popularity.append("0")
				track_duration.append(j['duration'])
				temp_tracks += 1
				temp_duration += j['duration']
				artist_song_num += 1
				artist_duration += j['duration']
			album_duration.append(temp_duration)
			album_tracks.append(temp_tracks)
			album_avg_song.append(float(temp_duration / temp_tracks))
		artist_avg_album_len = float(artist_song_num / artist_album_num)
		artist_avg_song_len = float(artist_duration / artist_song_num)
		artist_ref_count = len(json_data['references'])
	except Exception as e:
		return ""

	ret = artistData()
	ret.artist_name = artist_name.lower()
	ret.artist_pop = artist_pop
	ret.artist_sum = artist_sum
	ret.artist_content = artist_content
	ret.artist_followers = artist_followers
	ret.artist_genres = artist_genres
	ret.artist_album_num = artist_album_num
	ret.artist_song_num = artist_song_num
	ret.artist_duration = artist_duration
	ret.artist_avg_album_len = artist_avg_album_len
	ret.artist_avg_song_len = artist_avg_song_len
	ret.artist_ref_count = artist_ref_count
	ret.artist_sum_count = len(artist_sum.split(' '))
	ret.artist_content_count = len(artist_content.split(' '))
	ret.artist_sum_word = get_most_common_word(artist_sum)
	ret.artist_content_word = get_most_common_word(artist_content)
	ret.artist_albums = artist_albums
	ret.album_avg_song = album_avg_song
	ret.album_duration = album_duration
	ret.album_popularity = album_popularity
	ret.album_release = album_release
	ret.album_tracks = album_tracks
	ret.artist_track = artist_track
	ret.track_popularity = track_popularity
	ret.track_duration = track_duration
	return ret
	
def main():
	APP_NAME = "CS179G"

	conf = SparkConf().setAppName(APP_NAME)
	conf = conf.setMaster("spark://spark53.cs.ucr.edu:7077")
	#conf = conf.setMaster("local[*]")
	sc = SparkContext(conf=conf)
	sc.addFile("/home/cs179g/artistData.py")
	sqlContext = SQLContext(sc)
	test = sqlContext.read.json("fixed_info.json")
	test = test.map(lambda x: get_artist_stats(x))
	test = test.distinct().filter(lambda x: x is not None).filter(lambda x: x!= "").filter(lambda row: row.artist_name != "" and row.artist_name != None)
	temp = test.map(lambda row: {'name': row.artist_name,\
						'albums': row.artist_album_num,\
						'avg_album': row.artist_avg_album_len,\
						'avg_song': row.artist_avg_song_len,\
						'cont': row.artist_content,\
						'followers': row.artist_followers,\
						'genres': row.artist_genres,\
						'pop': row.artist_pop,\
						'songs': row.artist_song_num,\
						'sum': row.artist_sum,\
						'ref_count': row.artist_ref_count,\
						'duration': row.artist_duration,\
						'sum_word': row.artist_sum_word,\
						'cont_word': row.artist_content_word,\
						'sum_count': row.artist_sum_count,\
						'cont_count': row.artist_content_count,\
						'artist_albums': row.artist_albums,\
						'album_avg_song': row.album_avg_song,\
						'album_duration': row.album_duration,\
						'album_popularity': row.album_popularity,\
						'album_release': row.album_release ,\
						'album_tracks': row.album_tracks,\
						'artist_track': row.artist_track,\
						'track_popularity': row.track_popularity,\
						'track_duration': row.track_duration})
	temp.saveToCassandra(keyspace='data', table='all_data')

if __name__ == "__main__":
	main()
