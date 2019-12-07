import sys
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
import argparse
import requests
from bs4 import BeautifulSoup
import re
from selenium import webdriver
from selenium.common.exceptions import WebDriverException, NoSuchElementException, StaleElementReferenceException, TimeoutException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from functools import wraps


parser = argparse.ArgumentParser()
parser.add_argument(
    "--download_data", help="whether to download data or not",
   default=False, action='store_true'
)
parser.add_argument(
    "--cores", help="number of CPU cores to use",
    default=2, type=int
)
parser.add_argument(
    "--days", help="number of days of data to download starting from 2019-10-1, < 30 days",
    default=1, type=int
)
parser.add_argument(
    "--load_data", help="whether to load data into database",
    default=False, action='store_true'
)
parser.add_argument(
    "--neo4j_host", help="url to neo4j service",
    default='', type=str
)
parser.add_argument(
    "--neo4j_username", help="neo4j username",
    default='', type=str
)
parser.add_argument(
    "--neo4j_password", help="neo4j password",
    default='', type=str
)
parser.add_argument(
    "--create_graph", help="whether to create a new graph in neo4j",
    default=False, action='store_true'
)
parser.add_argument(
    "--scrape_topics", help="whether to scrape Github to get topics",
    default=False, action='store_true'
)
parser.add_argument(
    "--github_token", help="Github personal access token",
    default='', type=str
)
parser.add_argument(
    "--label_repos", help="Insert the labels from repo2category.pickle into database",
    default=False, action='store_true'
)
args = parser.parse_args()

do_download = args.download_data
N_PROCESSORS = args.cores
past_days = args.days
do_load = args.load_data
host = args.neo4j_host
neo4j_user = args.neo4j_username
pw =  args.neo4j_password
create_graph = args.create_graph
do_scrape = args.scrape_topics
OAUTH_TOKEN = args.github_token
do_label = args.label_repos

if (do_load or do_scrape or do_label) and not (host and neo4j_user and pw):
    print("Neo4j host, username and password should be specified by options --neo4j_host, --neo4j_username and --neo4j_password")
    sys.exit()

if do_scrape and not OAUTH_TOKEN:
    print("Github personal access token should be specified by options --github_token")
    sys.exit()

process_directory = '../data/processed'


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

def store_in_graph_db(date, graph_db, process_directory):
    #Store all users, repos, triplets for date in graph_db
    triplets = pd.read_csv(os.path.join(process_directory + '/' + date + '/triplets.csv'))
    triplets = triplets.groupby(['user_id', 'user_name', 'event_type', 'repo_id', 'repo_name'])[['user_id']].count()
    triplets = triplets.rename(columns = {"user_id": "count"}).reset_index()
    users = pd.read_csv(os.path.join(process_directory + '/' + date + '/users.csv'))
    repos = pd.read_csv(os.path.join(process_directory + '/' + date + '/repos.csv'))
    if path.exists(process_directory + '/' + 'users.pkl'):
        with open(os.path.join(process_directory + '/' + 'users.pkl'), 'rb') as f:
            user_set = pickle.load(f)        
    else:
        user_set = set()

    if path.exists(process_directory + '/' + 'repos.pkl'):
        with open(os.path.join(process_directory + '/' + 'repos.pkl'), 'rb') as f:
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

    with open(os.path.join(process_directory + '/' + 'users.pkl'), 'wb') as f:
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
    with open(os.path.join(process_directory + '/' + 'repos.pkl'), 'wb') as f:
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

    if create_graph:
        graph = create_graph_db(host, neo4j_user, pw)
    else: 
        graph = Graph(host, auth=(neo4j_user, pw))

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
    
    with open(os.path.join(process_directory + '/' + 'repos.pkl'), 'rb') as f:
        repo_set = pickle.load(f)      

    repo_name2id = {}    
    for item in repo_set:
        repo_id = item[0]
        repo_name = item[1]
        repo_name2id[repo_name] = repo_id

    with open(os.path.join(process_directory + '/' + 'repos_name2id.pkl'), 'wb') as f:
        pickle.dump(repo_name2id, f)

    with open(os.path.join(process_directory + '/' + 'users.pkl'), 'rb') as f:
        user_set = pickle.load(f)      

    user_name2id = {}    
    for item in user_set:
        user_id = item[0]
        user_name = item[1]
        user_name2id[user_name] = user_id

    with open(os.path.join(process_directory + '/' + 'users_name2id.pkl'), 'wb') as f:
        pickle.dump(user_name2id, f)


######################## SCRAPE TOPICS ########################


if do_scrape:

    #### SCRAPE GITHUB TOPICS PAGE

    BASE_URL = "https://github.com/topics/"

    MOST_STARS_SUFFIX = "?o=desc&s=stars"
    USE_MOST_STARS = True
    TIMEOUT = 3

    driver = webdriver.Chrome()
    driver.get(BASE_URL)

    def wait_get_element_by_class(class_name, driver = driver):
        return WebDriverWait(driver, timeout = TIMEOUT).until(
            EC.element_to_be_clickable(
                (By.CLASS_NAME, class_name)
            )
        )

    def wait_get_elements_by_class(class_name, driver = driver):
        return WebDriverWait(driver, timeout = TIMEOUT).until(
            EC.presence_of_all_elements_located(
                (By.CLASS_NAME, class_name)
            )
        )
    def wait_get_element_by_xpath(xpath, driver = driver):
        return WebDriverWait(driver, timeout = TIMEOUT).until(
            EC.element_to_be_clickable(
                (By.XPATH, xpath)
            )
        )
        
    def wait_get_elements_by_xpath(xpath, driver = driver):
        return WebDriverWait(driver, timeout = TIMEOUT).until(
            EC.presence_of_all_elements_located(
                (By.XPATH, xpath)
            )
        )

    # load more to expand all topics
    load_more_button = wait_get_element_by_class("ajax-pagination-btn")
    load_more_button.click()

    topic_elements = wait_get_elements_by_xpath("//li[contains(@class, 'py-4') and a[contains(@href, '/topics')]]")

    topic_links = {}

    for el in topic_elements:
        topic_link = el.find_element_by_tag_name("a").get_attribute("href")
        topic_name = el.find_element_by_class_name("f3").text
        topic_links[topic_name] = topic_link

    def retry(tries=2, delay=0.5, logger=None):
        """Retry calling the decorated function using an exponential backoff.

        http://www.saltycrane.com/blog/2009/11/trying-out-retry-decorator-python/
        original from: http://wiki.python.org/moin/PythonDecoratorLibrary#Retry

        :param ExceptionToCheck: the exception to check. may be a tuple of
            exceptions to check
        :type ExceptionToCheck: Exception or tuple
        :param tries: number of times to try (not retry) before giving up
        :type tries: int
        :param delay: initial delay between retries in seconds
        :type delay: int
        :param backoff: backoff multiplier e.g. value of 2 will double the delay
            each retry
        :type backoff: int
        :param logger: logger to use. If None, print
        :type logger: logging.Logger instance
        """
        def deco_retry(f):

            @wraps(f)
            def f_retry(*args, **kwargs):
                mtries, mdelay = tries, delay
                while mtries > 1:
                    try:
                        return f(*args, **kwargs)
                    except StaleElementReferenceException as e:
                        msg = "%s, Retrying in %d seconds..." % (str(e), mdelay)
                        print(msg)
                        time.sleep(mdelay)
                        mtries -= 1
                return f(*args, **kwargs)

            return f_retry  # true decorator

        return deco_retry

    @retry()
    def expand_more():
        load_more_button = wait_get_element_by_class("ajax-pagination-btn")
        load_more_button.click()

    def extract_abbreviated_repos(driver, extra_pages=0):
        for _ in range(extra_pages):
            try:
                expand_more()
            except (StaleElementReferenceException, TimeoutException):
                print("unable to load all extra pages")

        topic_repos = wait_get_elements_by_xpath("//a[@class='text-bold' and contains(@href, '/')]")
        repo_list = []

        for repo in topic_repos:
            repo_link = repo.get_attribute("href")
            abbreviated_repo = "/".join(repo_link.split("/")[-2:])
            repo_list.append(abbreviated_repo)
        return repo_list

    topic_repos_by_best_match = {}
    topic_repos_by_most_stars = {}
    related_topics = defaultdict(list)

    for name, link in tqdm(topic_links.items()):
        # best match by default
        #link = link + MOST_STARS_SUFFIX
        driver.get(link)
        topic_repos_by_best_match[name] = extract_abbreviated_repos(driver, 10)
        print(name, len(topic_repos_by_best_match[name]))
        # get most stars as well if enabled
        if USE_MOST_STARS:
            driver.get(link + MOST_STARS_SUFFIX)
            topic_repos_by_most_stars[name] = extract_abbreviated_repos(driver, 10)
        
        #find only the ones under related topics"
        related_topics_els = driver.find_elements_by_xpath("//a[contains(@class, 'topic-tag') and contains(@class, 'my-1')]")
        for topic_el in related_topics_els:
            related_topics[name].append(topic_el.text)

    results = {
        "topic_repos_by_best_match": topic_repos_by_best_match,
        "topic_repos_by_most_stars": topic_repos_by_most_stars,
        "related_topics": related_topics
    }
    with open("topic_repos.pickle", "wb") as handle:
        pickle.dump(results, handle)
    with open("topic_repos.pickle", "rb") as handle:
        results = pickle.load(handle)
        
    normalize_text = lambda text: re.sub(r"[^0-9a-zA-Z+#]+", "", text).lower()

    normalized_related_topics = defaultdict(list)
    for topic, related in related_topics.items():
        normalized_related_topics[normalize_text(topic)] = [
            normalize_text(t) for t in related
        ]

    all_topics = set()
    for topic, related in normalized_related_topics.items():
        all_topics.add(topic)
        for t in related:
            all_topics.add(t)

    normalized_related_topics_sets = {}
    for topic, related in normalized_related_topics.items():
        related_sets = set(related)
        related_sets.add(topic)
        normalized_related_topics_sets[topic] = related_sets
        
    IOU_results = {}
    for topic1, related1 in normalized_related_topics_sets.items():
        for topic2, related2 in normalized_related_topics_sets.items():
            if topic1 != topic2:
                IOU_results[(topic1, topic2)] = len(related1&related2)/len(related1|related2)

    IOU_rev_results = defaultdict(list)
    for comp, res in IOU_results.items():
        IOU_rev_results[res].append(comp)
        
    threshold = 0.1
    cluster = defaultdict(list)
    for score in sorted(IOU_rev_results.keys(), reverse=True):
        if score < threshold:
            break
        for comp in IOU_rev_results[score]:
            t1, t2 = comp
            if len(cluster[t1])==0 and len(cluster[t2])==0:
                cluster[t1] = [t1, t2]
                cluster[t2] = cluster[t1]
            else:
                if len(cluster[t2])==0:
                    cluster[t1].append(t2)
                    cluster[t2] = cluster[t1]
                elif len(cluster[t1])==0:
                    cluster[t2].append(t1)
                    cluster[t1] = cluster[t2]
                    
            print(comp, score)
        
    individual_clusters = set()
    topics_in_clusters = set()
    for val in cluster.values():
        individual_clusters.add(tuple(val))
        for t in val:
            topics_in_clusters.add(t)
            
    assignments = {
    'shell': ('bash', 'shell'),
    'xamarin':('mvvmcross', 'reactiveui', 'xamarin'),
    'gaming':('unity', 'unrealengine', 'minecraft'),
    'controllers':('arduino', 'raspberrypi', 'homebridge'),
    'web technologies': ('koa',
    'webpack',
    'redux',
    'graphql',
    'pwa',
    'electron',
    'nativescript',
    'githubapi',
    'restapi'),
    'vim':('spacevim', 'vim'),
    'net':('aspnet', 'net'),
    'data science': ('deeplearning',
    'machinelearning',
    'tensorflow',
    'scikitlearn',
    'naturallanguageprocessing',
    'datavisualization'),
    'android': ('kotlin', 'android'),
    'fast languages': ('lua', 'rust', 'nim'),
    'cryptocurrency': ('ethereum', 'ipfs'),
    'specialized language': ('go', 'r', 'c#' , 'c'),
    'iOS': ('objectivec', 'swift', 'ios'),
    'operating system': ('linux', 'macos', 'ubuntu'),
    'typescript': ('angular', 'typescript', 'storybook'),
    'microblogging': ('mastodon', 'twitter'),
    'font': ('font', 'iconfont'),
    'virtual environment management': ('ansible', 'vagrant'),
    'perl': ('perl', 'perl6'),
    'chat': ('telegram', 'emoji'),
    'mongoDB': ('mongodb', 'mongoose'),
    'emulator': ('azure', 'emulator'),
    'game engine' : ('gameengine', 'opengl'),
    'cryptocurrency': ('bitcoin', 'cryptocurrency', 'p2p'),
    'browsers': ('chrome', 'chromeextension', 'firefox'),
    'functional programming languages': ('clojure', 'haskell', 'elixir'),
    'javascript web': ('javascript', 'jquery', 'ajax', 'json', 'ember'),
    'frontend': ('frontend', 'bootstrap', 'css', 'html', 'react', 'vuejs'),
    'javascript compiler/linter': ('babel', 'eslint'),
    'scientific programming languages': ('matlab', 'thejulialanguage'),
    'text editor': ('emacs', 'atom', 'jupyternotebook'),
    'ruby web': ('ruby', 'rails'),
    'python web': ('django', 'flask', 'wagtail'),
    'database': ('database', 'mysql', 'sql', 'nosql', 'xml'),
    'php web': ('php', 'wordpress', 'wordplate', 'symfony', 'laravel'),
    'google web': ('firebase', 'amp'),
    'containerization': ('docker', 'kubernetes'),
    'typesetting language': ['latex'],
    }

    category2topic = {key: set(val) for key, val in assignments.items()}

    topics_in_category = set()
    for key, topics in category2topic.items():
        for t in topics:
            topics_in_category.add(t)
            
    normalized_topic_repos_by_best_match = {normalize_text(key): val for key,val in topic_repos_by_best_match.items()}
    normalized_topic_repos_by_most_stars = {normalize_text(key): val for key,val in topic_repos_by_most_stars.items()}

    # comment this out to not include
    for topic in normalized_topic_repos_by_best_match:
        if topic not in topics_in_category:
            category2topic[topic] = {topic}
            
    # check if any overlapping topic - there shouldn't be
    check_existing = set()
    for topics in category2topic.values():
        for t in topics:
            if t in check_existing:
                print("duplicate", t)
            else:
                check_existing.add(t)
                
    topic2category = {}
    for category, topics in category2topic.items():
        for t in topics:
            topic2category[t] = category
            
    to_scrape_topics = []
    for topic in topic2category.keys():
        if topic not in normalized_topic_repos_by_best_match:
            to_scrape_topics.append(topic)
            
    results_include_single_category = {
        "topic_repos_by_best_match": normalized_topic_repos_by_best_match,
        "topic_repos_by_most_stars": normalized_topic_repos_by_most_stars,
        "topic2category": topic2category,
        "category2topic": category2topic
    }

    with open("topic_cluster_with_single.pickle", "wb") as handle:
        pickle.dump(results_include_single_category, handle)

    #### SCAPRE REPOS STARRED BY MORE THAN 10

    graph = Graph(host, auth=(neo4j_user, pw))
    tx = graph.begin()

    query = (
    'MATCH (u:`User`)-[w:WatchEvent]-(r:`Repo`) '
    'WITH r, count(w) as rel_cnt '
    'WHERE rel_cnt > 10 '
    'RETURN r.repo_name, rel_cnt;')

    repos = tx.run(query).data()
    repos_df = pd.DataFrame(repos)
    repo_names = repos_df.name.tolist()
    repo_topics = []

    # don't hit the actual 5000 rate limit
    RATE_LIMIT = 4900
    SLEEP_TIME = 3600
    # sleep for one hour if hit rate limit
    api_call_headers = {'Authorization': 'token {}'.format(OAUTH_TOKEN)}

    current_request_count = 0
    recorded_time = time.time()
    for repo_name in tqdm(repo_names):
        resp = requests.get("https://www.github.com/{}".format(repo_name), headers=api_call_headers)
        if resp.status_code != 200:
            repo_topics.append([])
            print("{} cannot be accessed".format(repo_name))
            continue
        soup = BeautifulSoup(resp.content, "lxml")
        topics = soup.find_all("a", {"class":"topic-tag topic-tag-link "})
        repo_associated_topics = [topic.text.strip() for topic in topics]
        repo_topics.append(repo_associated_topics)
        
        # sleep to prevent from hitting rate limit
        current_request_count += 1
        if current_request_count == RATE_LIMIT:
            elapsed_time = time.time() - recorded_time
            sleep_time = SLEEP_TIME - elapsed_time
            if sleep_time > 0:
                print("sleeping for {}".format(sleep_time))
                time.sleep(sleep_time)

            # reset
            recorded_time = time.time()
            current_request_count = 0
        
    repo2topics = {name: topic for name, topic in zip(repo_names, repo_topics)}

    with open("high_star_repo2topics.pickle", "wb") as handle:
        pickle.dump(repo2topics, handle)
    with open("high_star_repo2topics.pickle", "rb") as handle:
        repo2topics = pickle.load(handle)
    with open("topic_cluster_with_single.pickle", "rb") as handle:
        results = pickle.load(handle)

    # top 30 best match repos for each topic
    # note that topics in this dict may contain more than you need
    # since it's all the data I scraped
    topic_repos_by_best_match = results['topic_repos_by_best_match']
    topic_repos_by_most_stars = results['topic_repos_by_most_stars']
    # you should loop over this dictionary for the topic/categories we're using
    # self explanatory: topic to the corresponding category
    topic2category = results["topic2category"]
    category2topic = results["category2topic"]

    normalize_text = lambda text: re.sub(r"[^0-9a-zA-Z+#]+", "", text).lower()

    repo2category = {}
    for repo_name, topics in repo2topics.items():
        categories_set = set(topic2category.get(normalize_text(t),"unknown") for t in topics)
        if len(categories_set) > 1 and "unknown" in categories_set:
            categories_set.remove("unknown")
        categories = list(categories_set)
        if len(categories) == 0:
            categories = ["unknown"]
        repo2category[repo_name] = categories

    with open("repo2category.pickle", "wb") as handle:
        pickle.dump(repo2category, handle)


######################## LABEL REPOS ########################


if do_label:
    with open(path.join(process_directory, 'repos_name2id.pkl'), 'rb') as f:
        repo_name2id = pickle.load(f)
    with open(path.join(process_directory, 'repo2category.pickle'), 'rb') as f:
        repo2category = pickle.load(f)

    graph = Graph(host, auth=(neo4j_user, pw))
    tx = graph.begin()
    for repo, categories in tqdm(repo2category.items()):
        if repo not in repo_name2id.keys():
            continue
        repo_id = repo_name2id[repo]
        query = ("MATCH (r:`Repo`{repo_id: {A}} ) "
                    "SET r.categories = {B} "
                    "RETURN r.repo_id"
                    )
        result = tx.run(query, {"A": repo_id, "B": categories})
    tx.commit()