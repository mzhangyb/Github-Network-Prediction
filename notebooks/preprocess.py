import sys
import os
import pandas as pd
import json
import gzip
# import wget
from collections import defaultdict
from variables import events

def preprocess(date, raw_directory, preprocess_directory): 
	"""
	Preprocess gharchive for given date. Date format should be YYYY-MM-DD
	"""
	repo_df = None
	user_df = None
	triplet_df = None
	for i in range(24):
		file_path = raw_directory + '/' + date + '/' + date + '-' + str(i) + '.json.gz'
		sub_repo_df, sub_user_df, sub_triplet_df = preprocess_file(file_path)
		if repo_df is None:
			repo_df = pd.DataFrame(columns = sub_repo_df.columns)
			user_df = pd.DataFrame(columns = sub_repo_df.columns)
			triplet_df = pd.DataFrame(columns = ['user_id', 'user_name', 'event_type', 'repo_id', 'repo_name'])
		repo_df = repo_df.append(sub_repo_df, sort = False)
		user_df = user_df.append(sub_user_df, sort= False)
		triplet_df = triplet_df.append(sub_triplet_df, sort = False)
	# Get total count in one day for each 
	repo_df = repo_df.groupby(['repo_id', 'repo_name']).sum().reset_index()
	user_df = user_df.groupby(['user_id', 'user_name']).sum().reset_index()
	if not os.path.exists(preprocess_directory):
		os.mkdir(preprocess_directory)
	if not os.path.exists(preprocess_directory + '/' + date):
		os.mkdir(preprocess_directory + '/' + date)
	repo_df.to_csv(preprocess_directory + '/' + date + '/' + 'repos.csv', index = False)
	user_df.to_csv(preprocess_directory + '/' + date + '/' + 'users.csv', index = False)
	triplet_df.to_csv(preprocess_directory + '/' + date + '/' + 'triplets.csv', index = False)

def preprocess_file(file_path):
	"""
	Preprocess a file. 
	Returns dataframes for:
		- users and corresponding counts of events 
		- repos and corresponding counts of events
		- user and repo for each event
	"""
	# Create dictionary of dictionary to keep track number of events for each repo/person.
	repo_dict = defaultdict(lambda: defaultdict(lambda: 0))
	user_dict = defaultdict(lambda: defaultdict(lambda: 0))
	# Create dictionary to map each id to name
	repo_id_to_name_dict = {}
	user_id_to_name_dict = {}
	triplet = []
	# Get count of each event for each repo and user. Also make triplets for network.
	with gzip.open(file_path) as f:
	    for line in f:
	        loaded_line = json.loads(line)
	        repo = loaded_line['repo']['id']
	        repo_name = loaded_line['repo']['name']
	        user = loaded_line['actor']['id']
	        user_name = loaded_line['actor']['login']
	        event_type = loaded_line['type']
	        repo_dict[event_type][repo] += 1
	        user_dict[event_type][user] += 1
	        user_id_to_name_dict[user] = user_name
	        repo_id_to_name_dict[repo] = repo_name
	        triplet.append((user, user_name, event_type, repo, repo_name))
	repo_df = create_events_count_df(repo_dict, repo_id_to_name_dict, "repo")
	user_df = create_events_count_df(user_dict, user_id_to_name_dict, "user")
	triplet_df = pd.DataFrame(triplet, columns = ['user_id', 'user_name', 'event_type', 'repo_id', 'repo_name'])
	return repo_df, user_df, triplet_df

def create_events_count_df(events_count_mapping, id_to_name_mapping, object_name):
	"""
	Make repo df with {object_name}_id, {object_name}_name, and count of each event
	"""
	df = pd.DataFrame(events_count_mapping, columns = events)
	df = df.reset_index()
	df = df.rename(columns = {'index': object_name + '_id'})
	df = df.merge(pd.DataFrame(id_to_name_mapping.items(), columns = [object_name + '_id', object_name + '_name']),
	                        how = 'right',
	                        on = [object_name + '_id']
	                       )
	# Reorder columns
	df = df[df.columns.tolist()[-1:] + df.columns.tolist()[:-1]]
	for event in events:
		df[event] = pd.to_numeric(df[event])
	df = df.fillna(0)
	return df

def main(argv):
	preprocess(argv[0], argv[1], argv[2])

if __name__ == '__main__':
	main(sys.argv[1:])
