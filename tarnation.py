import json, sys

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


o = open('fixed_info.json', 'w+')
f = open('info.txt', 'r')
lines = f.readlines()
for i in lines:
	new_line = correct_json(i)
	new_line = new_line[:len(new_line)-2]
	#print new_line
	#print json.dumps(new_line)
	o.write(new_line+"\n")
o.close()
f.close()
