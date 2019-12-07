# Github Explorer

Github Explorer is a course project for MIT 6.S080. It is an end-to-end exploration system that recommends users/repos based on common interest, categorizes users’ interests profile, and identifies rising stars in a field. This would allow users to further explore their interests and recruiters to identify candidates’ interests. 

# Installation

0. Dependencies:
* Install neo4j and create a user. Make sure to set `dbms.memory.heap.max_size=8G` in neo4j.conf. (It can be found under `<neo4j installation folder>/libexec/conf/`)
* Install Chrome and Selenium Chrome Driver (https://sites.google.com/a/chromium.org/chromedriver/downloads) if you want to scrape repo topics from Github. If you decide to use the topics we scraped, you can skip this step.
* Clone this repo and install python dependencies by `pip install -r requirements.txt`

1. `cd` into the `notebook` folder. Load Github event data into Neo4j database. The script `set_up_github_graph.py` does several tasks in loading data into Neo4j database. 
* Download and preprocess Github event data from GHArchive. The script does this task if given a `--download_data` flag. The arguments are: `--cores`, the number of cores will be used for downloading and preprocessing the data; `--days`, the number of days of data to download (starting from 2019-10-01). For example, to download and preprocess the data of 2019-10-01: `python set_up_github_graph.py --download_data --cores=2 --days=1` (For 1 day of data, this would take ~30 min)
* Load the users, repos, and events into the database. (Make sure the Neo4j instance is started.) The script does this task if given a `--load_data` flag. The arguments are: `--days`, the number of days of data to load (starting from 2019-10-01); `--neo4j_host`, the endpoint exposed by the Neo4j database; `--neo4j_username` and `--neo4j_password`, the credential for logging into Neo4j; `--create_graph`, whether to create a new graph, this flag should be on when loading into the database for the first time. For example, to load the data of 2019-10-01 we just downloaded: `python set_up_github_graph.py --load_data --days=1 --neo4j_host=http://localhost:7474 --neo4j_username=neo4j --neo4j_password=research --create_graph` (For 1 day of data, this would take ~45 min)
* (This step can be skipped if we are using the data we already scraped, they are in the data/processed folder of this repo) Scrape Github to get popular topics and topics of popular repos. The script does this task if given a `--scrape_topics` flag. The arguments are: `--github_token`, your Github access token for calling Github API; `--neo4j_host`, `--neo4j_username` and `--neo4j_password`. For example: `python set_up_github_graph.py --github_token --neo4j_host=http://localhost:7474 --neo4j_username=neo4j --neo4j_password=research`
* Insert label topics into the database. The script does this task if given a `--label_repos` flag. The arguments are `--neo4j_host`, `--neo4j_username` and `--neo4j_password`. For example: `python set_up_github_graph.py --label_repos --neo4j_host=http://localhost:7474 --neo4j_username=neo4j --neo4j_password=research`
* In `visualize_recommender.py` line 35, change `host`, `neo4j_user`, `pw` into your Neo4j credentials. 

# Exploring the Github Graph

Run the Dashboard by `python dasg_dashboard.py`.

The `Graph Layout` dropdown let us choose the graph layout algorithms. `Spring` would take much longer time to run compared with `Random`.

The `Explore` dropdown let us choose the aspects of the Github graph we want to explore:
* `Recommendation` helps us find repos and users we might be interested in. Under the Recommend dropdown, we can choose the type of recommendation (user-to-user, user-to-repo, repo-to-repo). A recommendation will be generated based on the text given to `Github user name or repo name` and the date range specified by `Activity Range`.
* `Rising Repos` and `Rising Users` help us discover trending repos and the users who are contributing to these repos. The specific topic we are interested in can be selected from `Rising repos for community` and `Rising users for community` dropdown.
* `User Profile` help us profiling the interests of a user. The username is entered under `Github username`. We can also adjust the `Commit/Push Weight` and `Star Weight` to specify how much are we focusing on his committing history or his starring history.
