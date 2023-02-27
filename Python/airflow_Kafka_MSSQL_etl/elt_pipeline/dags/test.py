import json
import os
import requests

my_dir = os.path.dirname(os.path.abspath(__file__))
configuration_file_path = os.path.join(my_dir, 'scores.json')
url = "https://odds.p.rapidapi.com/v4/sports/americanfootball_ncaaf/scores"
querystring = {"daysFrom": "3"}
headers = {
    "X-RapidAPI-Key": "ea61df56a8msh21c6250be4fb258p189a1djsn715926ac5b42",
    "X-RapidAPI-Host": "odds.p.rapidapi.com"
}
query = requests.request("GET", url, headers=headers, params=querystring)
query_list = query.text
query_list = query_list[1:-1]
if len(query_list) > 1:
    query_list = query_list.split(' {"id"')
query = [query_list[0]]
query_temp = ['{"id"' + e for e in query_list[1:]]
for i in range(len(query_list) - 1):
    query.append(query_temp[i])
print(query)
# for i in range(len(query_list) - 1):
#     query[i] = json.loads(query[i])
# scores_json_object = json.dumps(query, indent=4)
# with open(configuration_file_path, "w") as f:
#     f.write(scores_json_object)
