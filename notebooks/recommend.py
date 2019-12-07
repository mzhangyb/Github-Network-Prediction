import pandas as pd
import numpy as np
from tqdm import tqdm_notebook, tqdm
from py2neo import Graph
import pickle
from os import path

# Similarity between two users u1 and u2 is the proportion of starred repos they have in common
# The score of one given movie m is the proportion of users similar to u1 who rated m

#Adapted from https://www.kernix.com/blog/an-efficient-recommender-system-based-on-graph-database_p9

def recommend_users_to_user_id(tx, user_id, 
                               threshold = 0, k = None, start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (
            ### Similarity normalization : count number of repos starred by u1 ###
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT u2.user_id as user_id, u2.user_name as user_name, sim as score ORDER BY sim DESC')
    else:
        query = (### Similarity normalization : count number of repos starred by u1 ###
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT u2.user_id as user_id, u2.user_name as user_name, sim as score ORDER BY sim DESC LIMIT {k}')
    result = tx.run(query, {'user_id': user_id, 'threshold': threshold,
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    return result

def recommend_users_to_user_name(tx, user_name, 
                                 threshold = 0, k = None, start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (### Similarity normalization : count number of repos starred by u1 ###
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT u2.user_id as user_id, u2.user_name as user_name, sim as score ORDER BY sim DESC')
    else:
        query = (### Similarity normalization : count number of repos starred by u1 ###
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT u2.user_id as user_id, u2.user_name as user_name, sim as score ORDER BY sim DESC LIMIT {k}')
    result = tx.run(query, {'user_name': user_name, 'threshold': threshold,
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    return result

def recommend_repos_to_repo_id(tx, repo_id, 
                               threshold = 0, k = None, start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_id: {repo_id}}) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(u1) as count1 '
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_id: {repo_id}}) '
          'MATCH (r2:`Repo`)-[w2:`WatchEvent`]-(u1) '
          'WHERE NOT r1=r2 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH r2, count1, tofloat(count(w2)) as sim '
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT r2.repo_id as repo_id, r2.repo_name as repo_name, sim as score ORDER BY sim DESC')
    else:
        query = (
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_id: {repo_id}}) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(u1) as count1 '
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_id: {repo_id}}) '
          'MATCH (r2:`Repo`)-[w2:`WatchEvent`]-(u1) '
          'WHERE NOT r1=r2 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH r2, count1, tofloat(count(w2)) as sim '
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT r2.repo_id as repo_id, r2.repo_name as repo_name, sim as score ORDER BY sim DESC lIMIT {k}')
    result = tx.run(query, {'repo_id': repo_id, 'threshold': threshold,
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    return result

def recommend_repos_to_repo_name(tx, repo_name, 
                                 threshold = 0, k = None, start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_name: {repo_name}}) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(u1) as count1 '
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_name: {repo_name}}) '
          'MATCH (r2:`Repo`)-[w2:`WatchEvent`]-(u1) '
          'WHERE NOT r1=r2 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH r2, count1, tofloat(count(w2))  as sim '
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT r2.repo_id as repo_id, r2.repo_name as repo_name, sim as score ORDER BY sim DESC')
    else:
        query = (
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_name: {repo_name}}) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(u1) as count1 '
          'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_name: {repo_name}}) '
          'MATCH (r2:`Repo`)-[w2:`WatchEvent`]-(u1) '
          'WHERE NOT r1=r2 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH r2, count1, tofloat(count(w2)) as sim '
          'WHERE sim>{threshold} '
          # Compute score and return the list of suggestions ordered by score
          'RETURN DISTINCT r2.repo_id as repo_id, r2.repo_name as repo_name, sim as score ORDER BY sim DESC LIMIT {k}')
    result = tx.run(query, {'repo_name': repo_name, 'threshold': threshold,
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    return result

def recommend_repos_to_user_id(tx, user_id, threshold = 0, k = None,
                                 start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '  
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          'WITH u2, count1, tofloat(count(w2)) / count1 as sim '
          'WHERE sim>{threshold} '
          'WITH count(u2) as count2, count1 '
        ### Recommendation ###
        # Retrieve all users u2 who share at least one repo with u1
        'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
        'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
        'WHERE NOT u2=u1 '
        'AND {start_date} <= w.date <= {end_date} '
        'AND {start_date} <= w2.date <= {end_date} '
        # Compute similarity
        'WITH u1, u2,count2, tofloat(count(w2))/count1 as sim '
        # Keep users u2 whose similarity with u1 is above some threshold
        'WHERE sim>{threshold} '
        # Retrieve repos r that were rated by at least one similar user
        'MATCH (r:`Repo`)-[w2:`WatchEvent`]-(u2) '
        'WHERE {start_date} <= w2.date <= {end_date} '
        # Compute score and return the list of suggestions ordered by score
        'RETURN DISTINCT r.repo_id as repo_id, r.repo_name as repo_name, tofloat(count(w2)) as score ' 
        'ORDER BY score DESC'    
        )
        
    else:
        query = (
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '  
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) / count1 as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          'WITH count(u2) as count2, count1 '
        ### Recommendation ###
        # Retrieve all users u2 who share at least one movie with u1
        'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
        'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
        'WHERE NOT u2=u1 '
        'AND {start_date} <= w.date <= {end_date} '
        'AND {start_date} <= w2.date <= {end_date} '
        # Compute similarity
        'WITH u1, u2,count2, tofloat(count(w2))/count1 as sim '
        # Keep users u2 whose similarity with u1 is above some threshold
        'WHERE sim>{threshold} '
        # Retrieve repos r that were rated by at least one similar user, but not by u1
        'MATCH (r:`Repo`)-[w2:`WatchEvent`]-(u2) '
        'WHERE {start_date} <= w2.date <= {end_date} '
        # Compute score and return the list of suggestions ordered by score
        'RETURN DISTINCT r.repo_id as repo_id, r.repo_name as repo_name, tofloat(count(w2)) as score ' 
        'ORDER BY score DESC LIMIT {k}'    
        )
    
    result = tx.run(query, {'user_id': user_id, 'threshold': threshold, 
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    
    #Much easier to remove repos user have already starred outside of query.
    query = (
    'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
    'WHERE {start_date} <= w.date <= {end_date} '
    'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date, r1.repo_id as repo_id, r1.repo_name as repo_name'
    )
    user_stars = tx.run(query, {'user_id': user_id, 
                                        'start_date': start_date,
                                        'end_date': end_date}).data()
    user_starred_repos = set([star['repo_id'] for star in user_stars])
    result = [r for r in result if r['repo_id'] not in user_starred_repos]
    return result
    
def recommend_repos_to_user_name(tx, user_name, threshold = 0, k = None,
                                 start_date = '2019-10-01', end_date = '2019-10-31'):
    if k is None:
        query = (
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '  
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) / count1 as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          'WITH count(u2) as count2, count1 '
        'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
        'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
        'WHERE NOT u2=u1 '
        'AND {start_date} <= w.date <= {end_date} '
        'AND {start_date} <= w2.date <= {end_date} '
        # Compute similarity
        'WITH u1, u2,count2, tofloat(count(w2))/count1 as sim '
        # Keep users u2 whose similarity with u1 is above some threshold
        'WHERE sim>{threshold} '
        # Retrieve repos r that were rated by at least one similar user
        'MATCH (r:`Repo`)-[w2:`WatchEvent`]-(u2) '
        'WHERE {start_date} <= w2.date <= {end_date} '
        # Compute score and return the list of suggestions ordered by score
        'RETURN DISTINCT r.repo_id as repo_id, r.repo_name as repo_name, tofloat(count(w2)) as score ' 
        'ORDER BY score DESC'    
        )
    else:
        query = (
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'WHERE {start_date} <= w.date <= {end_date} '
          'WITH count(r1) as count1 '  
          ### Score normalization : count number of users who are considered similar to u1 ###
          # Retrieve all users u2 who share at least one starred repo with u1
          'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
          'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
          'WHERE NOT u2=u1 '
          'AND {start_date} <= w.date <= {end_date} '
          'AND {start_date} <= w2.date <= {end_date} '
          # Compute similarity
          'WITH u2, count1, tofloat(count(w2)) / count1 as sim '
          # Keep users u2 whose similarity with u1 is above some threshold
          'WHERE sim>{threshold} '
          'WITH count(u2) as count2, count1 '
        ### Recommendation ###
        'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
        'MATCH (r1)-[w2:`WatchEvent`]-(u2:`User`) '
        'WHERE NOT u2=u1 '
        'AND {start_date} <= w.date <= {end_date} '
        'AND {start_date} <= w2.date <= {end_date} '
        # Compute similarity
        'WITH u1, u2,count2, tofloat(count(w2))/count1 as sim '
        # Keep users u2 whose similarity with u1 is above some threshold
        'WHERE sim>{threshold} '
        # Retrieve repos r that were rated by at least one similar user
        'MATCH (r:`Repo`)-[w2:`WatchEvent`]-(u2) '
    #     'WHERE NOT (r)-[:`WatchEvent`]-(u1)'
        'WHERE {start_date} <= w2.date <= {end_date} '
        # Compute score and return the list of suggestions ordered by score
        'RETURN DISTINCT r.repo_id as repo_id, r.repo_name as repo_name, tofloat(count(w2)) as score ' 
        'ORDER BY score DESC LIMIT {k}'    
        )

    result = tx.run(query, {'user_name': user_name, 'threshold': threshold, 
                           'start_date': start_date, 'end_date': end_date, 'k': k}).data()
    
    #Much easier to remove repos user have already starred outside of query.
    query = (
    'MATCH (u1:`User` {user_name:{user_name}})-[w:`WatchEvent`]-(r1:`Repo`) '
    'WHERE {start_date} <= w.date <= {end_date} '
    'RETURN u1.user_name as user_name, u1.user_name as user_name, w.date as date, r1.repo_id as repo_id, r1.repo_name as repo_name'
    )
    user_stars = tx.run(query, {'user_name': user_name, 
                                        'start_date': start_date,
                                        'end_date': end_date}).data()
    user_starred_repos = set([star['repo_id'] for star in user_stars])
    result = [r for r in result if r['repo_id'] not in user_starred_repos]
    return result