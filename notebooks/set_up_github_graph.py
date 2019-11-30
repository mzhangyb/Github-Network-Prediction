"""
This script download data from GHArchive and process and load them into Neo4j
database. It assumes:
    1. We are using MacOS and the directory ../data/process is already created.
    2. A Neo4j server has be started.

To download and process data without loading them into Neo4j database:
    python set_up_github_graph.py --download_data --cores=<number of cpu cores to use> --past_days=<number of days of data to download>
e.g.
    python set_up_github_graph.py --download_data --cores=2 --past_days=1

To load processed data into Neo4j database:
    python set_up_github_graph.py --load_data --neo4j_host=<neo4j endpoint> --neo4j_username=<neo4j username> --neo4j_password=<neo4j password> --create_graph
e.g.
    python set_up_github_graph.py --load_data --neo4j_host=http://localhost:7474 --neo4j_username=neo4j --neo4j_password=research --create_graph

Or do the above two things together:
    python set_up_github_graph.py --download_data --cores=<number of cpu cores to use> --past_days=<number of days of data to download> --load_data --neo4j_host=<neo4j endpoint> --neo4j_username=<neo4j username> --neo4j_password=<neo4j password> --create_graph
e.g.
    python set_up_github_graph.py --download_data --cores=2 --past_days=1 python set_up_github_graph.py --load_data --neo4j_host=http://localhost:7474 --neo4j_username=neo4j --neo4j_password=research --create_graph

(Downloading and processing 1-day worth of data takes ~ hour on my 2 core Mac.)
"""


import datetime
import os
import time
from collections import defaultdict
from multiprocessing import Pool
import dask
import dask.bag as db
import dask.dataframe as dd
import json
import multiprocessing
import numpy as np
import pandas as pd
from tqdm import tqdm
from variables import events
from py2neo import Graph
import pickle
import networkx as nx
from os import path
import time
from collections import defaultdict
import argparse


parser = argparse.ArgumentParser()
parser.add_argument(
    "--download_data", help="whether to download data or not",
   default=False, action='store_true'
)
parser.add_argument(
    "--cores", help="number of CPU cores to use",
    type=int
)
parser.add_argument(
    "--past_days", help="number of days of data to download starting from 2019-10-1, < 30 days",
    type=int
)
parser.add_argument(
    "--load_data", help="whether to load data into database",
    default=False, action='store_true'
)
parser.add_argument(
    "--neo4j_host", help="url to neo4j service",
    type=str
)
parser.add_argument(
    "--neo4j_username", help="neo4j username",
    type=str
)
parser.add_argument(
    "--neo4j_password", help="neo4j password",
    type=str
)
parser.add_argument(
    "--create_graph", help="whether to create a new graph in neo4j",
    default=False, action='store_true'
)
parser.add_argument(
    "--label_prop", help="whether to propagate labels",
    default=False, action='store_true'
)
args = parser.parse_args()

do_download = args.download_data if args.download_data else False
N_PROCESSORS = args.cores if args.cores else 2
past_days = args.past_days if args.past_days else 1

do_load = args.load_data if args.load_data else False
host = args.neo4j_host if args.neo4j_host else None
neo4j_user = args.neo4j_username if args.neo4j_username else None
pw =  args.neo4j_password if args.neo4j_password else None
create_graph = args.create_graph if args.create_graph else False

do_prop = args.label_prop if args.label_prop else False

if (do_load or do_prop) and not (host and neo4j_user and pw):
    print("Neo4j host, username and password should be specified by options --neo4j_host, --neo4j_username and --neo4j_password")

process_directory = '../data/processed/'


######################## DOWNLOAD & PREPROCESS ########################


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


def jsons_to_repo_user_triplet_df(data):
    repo_dict = defaultdict(lambda: defaultdict(lambda: 0))
    user_dict = defaultdict(lambda: defaultdict(lambda: 0))
    # Create dictionary to map each id to name
    repo_id_to_name_dict = {}
    user_id_to_name_dict = {}
    triplet = []
    for loaded_line in data:
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


def url_to_repo_user_triplet_df(url):
    jsons = db.read_text(url).map(json.loads)
    return jsons_to_repo_user_triplet_df(jsons.compute())


datetime_to_str = lambda dt: dt.strftime("%Y-%m-%d")
ARCHIVE_URL = 'https://data.gharchive.org/{date}-{hour}.json.gz'
def compose_url(date, hour):
    return ARCHIVE_URL.format(date=date, hour=hour)


def save_output_list_to_dfs(outputs_dfs, save_folder):
    repo_df_list = []
    user_df_list = []
    triplet_df_list = []
    for sub_repo_df, sub_user_df, sub_triplet_df in outputs_dfs:
        repo_df_list.append(sub_repo_df)
        user_df_list.append(sub_user_df)
        triplet_df_list.append(sub_triplet_df)
        
    repo_df = pd.concat(repo_df_list).groupby(['repo_id', 'repo_name']).sum().reset_index()
    user_df = pd.concat(user_df_list).groupby(['user_id', 'user_name']).sum().reset_index()
    triplet_df = pd.concat(triplet_df_list)
        
    if not os.path.exists(save_folder):
        os.mkdir(save_folder)

    repo_df.to_csv(os.path.join(save_folder, 'repos.csv'), index=False)
    user_df.to_csv(os.path.join(save_folder, 'users.csv'), index=False)
    triplet_df.to_csv(os.path.join(save_folder, 'triplets.csv'), index = False)


# since daemonic processes are not allowed to have children
# we use non daemon pool to parallelize
# code from: https://stackoverflow.com/questions/6974695/python-process-pool-non-daemonic
class NonDaemonPool(multiprocessing.pool.Pool):
    def Process(self, *args, **kwds):
        proc = super(NonDaemonPool, self).Process(*args, **kwds)

        class NonDaemonProcess(proc.__class__):
            """Monkey-patch process to ensure it is never daemonized"""

            @property
            def daemon(self):
                return False

            @daemon.setter
            def daemon(self, val):
                pass

        proc.__class__ = NonDaemonProcess

        return proc

if do_download:
    print(f"Downloading and processing {past_days} days of data with {N_PROCESSORS} cores...")
    start_date = datetime.datetime.strptime("2019-09-30", '%Y-%m-%d') #datetime.datetime.today()
    save_dir = process_directory
    p = NonDaemonPool(N_PROCESSORS)
    for day in tqdm(range(1, past_days + 1)):
        current_date = start_date + datetime.timedelta(day)
        # hour
        current_date_str = datetime_to_str(current_date)
        print("Processing {}".format(current_date_str))
        current_date_urls = [compose_url(current_date_str, hour) for hour in range(24)]
        # list of DF ouputs
        outputs_dfs = p.map(url_to_repo_user_triplet_df, current_date_urls)
        
        save_folder = os.path.join(save_dir, current_date_str)
        save_output_list_to_dfs(outputs_dfs, save_folder)


######################## LOAD TO DATABASE ########################


def create_graph_db(host, user = 'neo4j', pw = 'research'):
    #Still need to figure out if it's possible to launch neo4j from python.
    graph = Graph(host, auth=(user, pw))
    #create user and repo node constraint
    graph.run('CREATE INDEX ON :User(user_id)')
    graph.run('CREATE INDEX ON :Repo(repo_id)')
    return graph

if do_load and create_graph:
    graph = create_graph_db(host, neo4j_user, pw)
else: 
    graph = Graph(host, auth=(neo4j_user, pw))

def store_in_graph_db(date, graph_db, process_directory):
    #Store all users, repos, triplets for date in graph_db
    triplets = pd.read_csv(process_directory + '/' + date + '/triplets.csv')
    triplets = triplets.groupby(['user_id', 'user_name', 'event_type', 'repo_id', 'repo_name'])[['user_id']].count()
    triplets = triplets.rename(columns = {"user_id": "count"}).reset_index()
    users = pd.read_csv(process_directory + '/' + date + '/users.csv')
    repos = pd.read_csv(process_directory + '/' + date + '/repos.csv')
    if path.exists(process_directory + '/' + 'users.pkl'):
        with open(process_directory + '/' + 'users.pkl', 'rb') as f:
            user_set = pickle.load(f)        
    else:
        user_set = set()

    if path.exists(process_directory + '/' + 'repos.pkl'):
        with open(process_directory + '/' + 'repos.pkl', 'rb') as f:
            repo_set = pickle.load(f)        
    else:
        repo_set = set()
    
    tx = graph.begin(autocommit=False)
    user_statement = "MERGE (a:`User`{user_id:{A}, user_name:{B}}) RETURN a"
    for i, row in tqdm(users.iterrows(), total = len(users)):
        user_id = row['user_id']
        user_name = row['user_name']
        if (user_id, user_name) not in user_set:
            user_set.add((user_id, user_name))
            tx.run(user_statement, {"A": user_id, "B": user_name})
            if i % 200 == 0:
                tx.process()
    tx.commit()

    with open(process_directory + '/' + 'users.pkl', 'wb') as f:
        pickle.dump(user_set, f)     
    
    tx = graph.begin(autocommit=False)
    repo_statement = "MERGE (a:`Repo`{repo_id:{A}, repo_name:{B}}) RETURN a"
    for i, row in tqdm(repos.iterrows(), total = len(repos)):
        repo_id = row['repo_id']
        repo_name = row['repo_name']
        if (repo_id, repo_name) not in repo_set:
            repo_set.add((repo_id, repo_name))
            tx.run(repo_statement, {"A": repo_id, "B": repo_name})
            if i % 200 == 0:
                tx.process()
    tx.commit()
    with open(process_directory + '/' + 'repos.pkl', 'wb') as f:
        pickle.dump(repo_set, f)     
    
    tx = graph.begin(autocommit=False)
    edge_statement1 = ("MATCH (u:`User`{user_id:{A}}) "
                         "MATCH (r:`Repo`{repo_id:{B}}) MERGE (u)-[e:`")
    edge_statement2 =  "`{date:{C}, count:{D}}]-(r) RETURN e"
    for i, row in tqdm(triplets.iterrows(), total = len(triplets)) :
        edge_statement = edge_statement1 + row['event_type'] + edge_statement2
        tx.run(edge_statement, {"A": row['user_id'], "B": row['repo_id'], "C": date, "D": row['count']})
        if i % 200 == 0:
            tx.process()
    tx.commit()

if do_load:
    dates = []
    for i in range(1, past_days+1):
        if i < 10:
            day = '0' + str(i)
        else:
            day = str(i)
        dates.append('2019-10-' + day)

    for date in dates:
        print(date)
        store_in_graph_db(date, graph, process_directory)


######################## PROPOGATE LABELS ########################


# with open('../data/processed/repos_name2id.pkl', 'rb') as f:
#     repo_name2id = pickle.load(f)    

# # this includes all the single topic category from the feature github
# # so it's clustered topics + single topic
# with open("old_topic_cluster_with_single.pickle", "rb") as handle:
#     results = pickle.load(handle)

# # top 30 best match repos for each topic
# # note that topics in this dict may contain more than you need
# # since it's all the data I scraped
# topic_repos_by_best_match = results['topic_repos_by_best_match']
# topic_repos_by_most_stars = results['topic_repos_by_most_stars']
# # you should loop over this dictionary for the topic/categories we're using
# # self explanatory: topic to the corresponding category
# topic2category = results["topic2category"]
# category2topic = results["category2topic"]

# #sort by descending order such that any overlapping repos can go to categories with lower number of repos.
# category2repo_list = [[key, value] for key, value in category2repo.items()]
# category2repo_list = sorted(category2repo_list, key = lambda x: len(x[1]), reverse = True)
# category2repo = {key: value for key, value in category2repo_list}
# id2category = {i: category for i, category in enumerate(category2repo.keys())}
# category2id = {category: i for i, category in id2category.items()}


# def label_propagation(graph, category2repo, remove_community = False):
#     if remove_community:
#         tx = graph.begin()
#         #Delete initial communities
#         query = ("MATCH(r:`Repo`) "
#                  "REMOVE r.community")
#         result = tx.run(query)
#         tx.commit()
        
#         tx =graph.begin()
#         query = ("MATCH(r:`Repo`) "
#                  "REMOVE r.seed_label")
#         result = tx.run(query)
#         tx.commit()
        
#         tx = graph.begin()
#         query = ("MATCH(u:`User`) "
#                  "REMOVE u.community")
#         result = tx.run(query)

#         tx.commit()
    
#     #set initial seeds for repos
#     tx = graph.begin()
#     for category, repos in tqdm(category2repo.items(), total = len(category2repo)):
#         if len(repos) > 200:
#             start_idx = 0
#             end_idx = 5
#         else: 
#             start_idx = 0
#             end_idx = 50
#         for repo_id in list(repos)[start_idx:end_idx]:
#             query = ("MATCH (r:`Repo`{repo_id: {A}} ) "
#                     "SET r.seed_label = {B} "
#                     "RETURN r.repo_id"
#                     )
#             result = tx.run(query, {"A": repo_id, "B": category2id[category]})
#     tx.commit()
#     tx = graph.begin()

#     query = ("CALL algo.labelPropagation(null, 'WatchEvent', {iterations:2, "
#             "weightProperty: 'count', seedProperty:'seed_label', direction:'BOTH', writeProperty:'community', write:true, concurrency:4}) "
#             "YIELD iterations, didConverge, loadMillis, computeMillis, writeMillis, write, weightProperty, writeProperty, " 
#             "communityCount, p25, p50, p75, p100"
#             )    
#     result = tx.run(query)
 
#     result = tx.run(query).data()

#     return result

# if do_prop:
#     result = label_propagation(graph, category2repo, True)

#     tx = graph.begin()
#     data = {}
#     for category, repos in tqdm(category2repo.items(), total = len(category2repo)):
#         data[category] = {}
#         for repo_id in repos:
#             query = ("MATCH (r:`Repo`{repo_id: {A}} ) "
#                     "RETURN r.community"
#                     )
#             result = tx.run(query, {"A": repo_id}).data()[0]['r.community']
#             if result is not None and result not in data[category]:
#                 data[category][result] = 0
#             if result is not None:
#                 data[category][result] += 1

#     #see how many of initial categories remain
#     final_categories = {}
#     for category, community in data.items():
#         #initial category may be in non-predefined category
#         try:
#             final_categories[category] = id2category[max(community, key= community.get)]
#         except:
#             final_categories[category] = max(community, key= community.get)

#     #final categories. awesomelists will go to 
#     s = set()
#     for cat in final_categories.values():
#         if type(cat) == str:
#             s.add(cat)
#     final_category2id = {}
#     for cat in s:
#         final_category2id[cat] = category2id[cat]

#     with open('../data/processed/community2id.pkl', 'wb') as f:
#         pickle.dump(final_category2id, f)