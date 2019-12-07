import os
import pickle
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output
import networkx as nx
import plotly.graph_objs as go
from py2neo import Graph
from datetime import datetime as dt
from recommend import *
from visualize_recommender import *
from collections import defaultdict

#THIS PART SHOULD BE MORE ADAPTABLE
process_directory = '../data/processed/'
with open(process_directory + '/' + 'repos_name2id.pkl', 'rb') as f:
    repo_name2id = pickle.load(f)
    repo_id2name = {i: repo for repo, i in repo_name2id.items()}

with open(process_directory + '/' + 'users_name2id.pkl', 'rb') as f:
    user_name2id = pickle.load(f)
    user_id2name = {i: user for user, i in user_name2id.items()}

with open(process_directory + '/' + 'all_categories.pkl', 'rb') as f:
# with open(process_directory + '/' + 'community2id.pkl', 'rb') as f:
    community2id = pickle.load(f)
    community2id = sorted(list(community2id))
    
# #all of communities not in the predefined ones will be unknown
# id2community = {i: community for community, i in community2id.items()}
# id2community = defaultdict(lambda: 'unknown', id2community)


#Change these to your own neo4j credentials
# host = "bolt://localhost:7687"
host = "http://localhost:7474"
neo4j_user = "neo4j"
pw =  "research"
graph = Graph(host, auth=(neo4j_user, pw))
tx = graph.begin()

def visualize_repo_recommendations_to_user(user_name, k, start_date, end_date,
                                           layout = 'spring', threshold = 0,):
    if user_name in user_name2id:
        user_id = user_name2id[user_name]
        query = (
            'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date, r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        #get all repos starred by user
        user_stars = tx.run(query, {'user_id': user_id, 
                                            'start_date': start_date,
                                            'end_date': end_date}).data()
        user_starred_repos = [star['repo_id'] for star in user_stars]
        #get recommendations for user
        user_recommendations = recommend_repos_to_user_id(tx, user_id, start_date = start_date, end_date = end_date, k = k)
        repos_recommended = [recommendation['repo_id'] for recommendation in user_recommendations]
        #get similar users
        similar_users = recommend_users_to_user_id(tx, user_id, start_date = start_date, end_date = end_date)
        similar_users_id = [user['user_id'] for user in similar_users]
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'AND u1.user_id IN {users} AND r1.repo_id IN {repos} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date,'
            'r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        #keep neighbors that are involved in recommendation
        similar_users = tx.run(query, {'users': similar_users_id,
                         'repos': repos_recommended,
                        'start_date': start_date,
                        'end_date': end_date}).data()

        similar_users_id = [user['user_id'] for user in similar_users]

        #get local network of user + starred repos + neighbors of starred repo + recommended repos
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'AND u1.user_id IN {users} AND r1.repo_id IN {repos} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date,'
            'r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        recommendation_network = tx.run(query, {'users': similar_users_id,
                         'repos': user_starred_repos + repos_recommended,
                        'start_date': start_date,
                        'end_date': end_date}).data()

        network_df = pd.DataFrame(user_stars + recommendation_network)
        if len(network_df):
            #Make nx.Graph object
            #Add edges  
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        edge_attr = 'date',
                                        create_using = nx.Graph())

            #Make node attributes (node_type, name, number of edges)
            G.add_node(user_id, node_type = 'user', name = user_name, color = 'red')
            for user in similar_users:
                G.add_node(user['user_id'], node_type = 'other user', name = user['user_name'], color = 'orange')

            for star in user_stars:
                G.add_node(star['repo_id'], node_type = 'user starred repo', name = star['repo_name'], color = 'blue')

            for recommendation in user_recommendations: 
                G.add_node(recommendation['repo_id'], node_type = 'recommended repos', name = recommendation['repo_name'],
                          color = 'green')
            
            
            if layout == 'spring':
                pos = nx.spring_layout(G)
            else: 
                pos = nx.random_layout(G)

            edge_trace = go.Scatter(
                x=[],
                y=[],
                line=dict(width=0.5,color='#888'),
                hoverinfo='none',
                mode='lines', name = 'Star Activity')

            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])

            user_node_trace = go.Scatter(
                x=[pos[user_id][0]],
                y=[pos[user_id][1]],
                text=[user_name],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='red',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )

            user_star_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='blue',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User Starred Repos', name = 'User Starred Repo'
            )

            for node in user_starred_repos:
                x, y = pos[node]
                user_star_trace['x'] += tuple([x])
                user_star_trace['y'] += tuple([y])
                user_star_trace['text'] += tuple([G.nodes[node]['name']])

            similar_user_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='orange',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Similar Users', name = 'Similar User'
            )

            for node in similar_users_id:
                x, y = pos[node]
                similar_user_trace['x'] += tuple([x])
                similar_user_trace['y'] += tuple([y])
                similar_user_trace['text'] += tuple([G.nodes[node]['name']])


            repos_recommended_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='green',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Recommended Repos', name = 'Recommended Repo'
            )

            for node in repos_recommended:
                x, y = pos[node]
                repos_recommended_trace['x'] += tuple([x])
                repos_recommended_trace['y'] += tuple([y])
                repos_recommended_trace['text'] += tuple([G.nodes[node]['name']])

            fig = go.Figure(data=[edge_trace, user_node_trace, user_star_trace, similar_user_trace, repos_recommended_trace],
                         layout=go.Layout(
                            titlefont=dict(size=16),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            return html.Div("Recommendations for {}".format(user_name)), generate_table(pd.DataFrame(user_recommendations)[['repo_name', 'score']]), dcc.Graph('recommender_graph', figure = fig)
    return ['None', None, None]

def visualize_user_recommendations_to_user(user_name, k, start_date, end_date,
                                           layout = 'spring', threshold = 0):

    if user_name in user_name2id:
        user_id = user_name2id[user_name]
        query = (
            'MATCH (u1:`User` {user_id:{user_id}})-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date, r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        #get all repos starred by user
        user_stars = tx.run(query, {'user_id': user_id, 
                                            'start_date': start_date,
                                            'end_date': end_date}).data()
        user_starred_repos = [star['repo_id'] for star in user_stars]
        #get similar users
        similar_users = recommend_users_to_user_id(tx, user_id, start_date = start_date, end_date = end_date, k = k)
        similar_users_id = [user['user_id'] for user in similar_users]
        #get local network of user + starred repos + similar users
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'AND u1.user_id IN {users} AND r1.repo_id IN {repos} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date,'
            'r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        recommendation_network = tx.run(query, {'users': similar_users_id,
                         'repos': user_starred_repos,
                        'start_date': start_date,
                        'end_date': end_date}).data()

        network_df = pd.DataFrame(user_stars + recommendation_network)
        if len(network_df):
            #Make nx.Graph object
            #Add edges  
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        edge_attr = 'date',
                                        create_using = nx.Graph())

            #Make node attributes (node_type, name, number of edges)
            G.add_node(user_id, node_type = 'user', name = user_name, color = 'red')
            for user in similar_users:
                G.add_node(user['user_id'], node_type = 'other user', name = user['user_name'], color = 'orange')

            for star in user_stars:
                G.add_node(star['repo_id'], node_type = 'user starred repo', name = star['repo_name'], color = 'blue')

            if layout == 'spring':
                pos = nx.spring_layout(G)
            else: 
                pos = nx.random_layout(G)
                
            edge_trace = go.Scatter(
                x=[],
                y=[],
                line=dict(width=0.5,color='#888'),
                hoverinfo='none',
                mode='lines', name = 'Star Activity')

            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])

            user_node_trace = go.Scatter(
                x=[pos[user_id][0]],
                y=[pos[user_id][1]],
                text=[user_name],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='red',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )

            user_star_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='blue',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User Starred Repo', name = 'User Starred Repo'
            )

            for node in user_starred_repos:
                x, y = pos[node]
                user_star_trace['x'] += tuple([x])
                user_star_trace['y'] += tuple([y])
                user_star_trace['text'] += tuple([G.nodes[node]['name']])

            similar_user_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='orange',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Similar User', name = 'Similar User'
            )

            for node in similar_users_id:
                x, y = pos[node]
                similar_user_trace['x'] += tuple([x])
                similar_user_trace['y'] += tuple([y])
                similar_user_trace['text'] += tuple([G.nodes[node]['name']])

            fig = go.Figure(data=[edge_trace, user_node_trace, user_star_trace, similar_user_trace],
                         layout=go.Layout(
                            titlefont=dict(size=16),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            return html.Div("Recommendations for {}".format(user_name)), generate_table(pd.DataFrame(similar_users)[['user_name', 'score']]), dcc.Graph('recommender_graph', figure = fig)
    return ['None', None, None]

def visualize_repo_recommendations_to_repo(repo_name, k, start_date, end_date,
                                           layout = 'spring', threshold = 0):

    if repo_name in repo_name2id:
        repo_id = repo_name2id[repo_name]
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo` {repo_id:{repo_id}}) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date, r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        #get all users starring the repo
        user_stars = tx.run(query, {'repo_id': repo_id, 
                                            'start_date': start_date,
                                            'end_date': end_date}).data()
        users = [star['user_id'] for star in user_stars]
        #get similar repos
        similar_repos = recommend_repos_to_repo_id(tx, repo_id, start_date = start_date, end_date = end_date, k = k)
        similar_repos_id = [repo['repo_id'] for repo in similar_repos]
        #get local network of repo + users who starred repo + similar repos
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            'AND u1.user_id IN {users} AND r1.repo_id IN {repos} '
            'RETURN u1.user_id as user_id, u1.user_name as user_name, w.date as date,'
            'r1.repo_id as repo_id, r1.repo_name as repo_name'
        )

        recommendation_network = tx.run(query, {'users': users,
                         'repos': similar_repos_id,
                        'start_date': start_date,
                        'end_date': end_date}).data()

        network_df = pd.DataFrame(user_stars + recommendation_network)
        if len(network_df):
            #Make nx.Graph object
            #Add edges  
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        edge_attr = 'date',
                                        create_using = nx.Graph())

            #Make node attributes (node_type, name, number of edges)
            G.add_node(repo_id, node_type = 'repo', name = repo_name, color = 'red')
            for repo in similar_repos:
                G.add_node(repo['repo_id'], node_type = 'recommended repo', name = repo['repo_name'], color = 'orange')

            for star in user_stars:
                G.add_node(star['user_id'], node_type = 'user', name = star['user_name'], color = 'blue')

            if layout == 'spring':
                pos = nx.spring_layout(G)
            else: 
                pos = nx.random_layout(G)
                
            edge_trace = go.Scatter(
                x=[],
                y=[],
                line=dict(width=0.5,color='#888'),
                hoverinfo='none',
                mode='lines', name = 'Star Activity')

            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])

            repo_node_trace = go.Scatter(
                x=[pos[repo_id][0]],
                y=[pos[repo_id][1]],
                text=[repo_name],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='red',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Repo', name = 'Repo'
            )

            user_star_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='blue',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )

            for node in users:
                x, y = pos[node]
                user_star_trace['x'] += tuple([x])
                user_star_trace['y'] += tuple([y])
                user_star_trace['text'] += tuple([G.nodes[node]['name']])

            similar_repo_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='orange',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Similar Repo', name = 'Similar Repo'
            )

            for node in similar_repos_id:
                x, y = pos[node]
                similar_repo_trace['x'] += tuple([x])
                similar_repo_trace['y'] += tuple([y])
                similar_repo_trace['text'] += tuple([G.nodes[node]['name']])

            fig = go.Figure(data=[edge_trace, repo_node_trace, user_star_trace, similar_repo_trace],
                         layout=go.Layout(
                            titlefont=dict(size=16),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            return html.Div("Recommendations for {}".format(repo_name)), generate_table(pd.DataFrame(similar_repos)[['repo_name', 'score']]), dcc.Graph('recommender_graph', figure = fig)
    return ['None', None, None]


def explode(df, lst_cols, fill_value=''):
    #https://stackoverflow.com/questions/45846765/efficient-way-to-unnest-explode-multiple-list-columns-in-a-pandas-dataframe
    # make sure `lst_cols` is a list
    if lst_cols and not isinstance(lst_cols, list):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)

    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()

    if (lens > 0).all():
        # ALL lists in cells aren't empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .loc[:, df.columns]
    else:
        # at least one list in cells is empty
        return pd.DataFrame({
            col:np.repeat(df[col].values, df[lst_cols[0]].str.len())
            for col in idx_cols
        }).assign(**{col:np.concatenate(df[col].values) for col in lst_cols}) \
          .append(df.loc[lens==0, idx_cols]).fillna(fill_value) \
          .loc[:, df.columns]
    

def visualize_rising_stars_repos(category, k, start_date, end_date, layout = 'spring'):
    #Get top repos in category 
    if category in community2id:
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} AND {community} IN r1.categories '            
            'WITH r1, count(w) as num_stars '
            'RETURN r1.repo_name as repo_name, num_stars as total_stars '
            'ORDER BY total_stars DESC LIMIT {k}'
        )
        result = tx.run(query, {'community': category, 
                                    'start_date': start_date,
                                    'end_date': end_date,
                                    'k': k,
                                   }).data()
        result_df = pd.DataFrame(result)
        if len(result_df):
            G = nx.Graph()
            for i, row in result_df.iterrows():
                G.add_node(row['repo_name'], stars = row['total_stars'])
            repos = result_df['repo_name'].unique()
            if layout == 'spring':
                pos = nx.spring_layout(G)
            else:
                pos = nx.random_layout(G)
            repo_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='orange',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Repo', name = 'Repo'
            )
            
            #add information about name and num stars to each repo node
            for node in repos:
                x, y = pos[node]
                repo_trace['x'] += tuple([x])
                repo_trace['y'] += tuple([y])
                text = node
                text += '<br>Number of stars: '
                text += str(G.nodes[node]['stars'])
                repo_trace['text'] += tuple([text])
            
            fig = go.Figure(data=[repo_trace],
                                     layout=go.Layout(
                                        titlefont=dict(size=16),
                                        showlegend=True,
                                        hovermode='closest',
                                        margin=dict(b=20,l=5,r=5,t=40),
                                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            return [html.Div("Rising Repos for {}".format(category)), generate_table(result_df[['repo_name', 'total_stars']]), dcc.Graph('rising_repos_graph', figure = fig)]
    print("Not valid")
    return ["None", None, None]


#Rising stars for users
def visualize_rising_stars(category, k, start_date, end_date, layout = 'spring'):
    #Label propagation only allowed numeric communities, but switched to manual
    # print(community2id)
    if category in community2id:
#         category_id = community2id[category]
#         query = (
#             'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`{community:{community}}) '
#             'WHERE {start_date} <= w.date <= {end_date} '            
#             'WITH r1, count(w) as num_stars '
#             'MATCH (u2:`User`)-[p:`PushEvent`]-(r1) '
#             'WHERE {start_date} <= p.date <= {end_date} '
#             'WITH DISTINCT u2.user_id as user_id, r1.repo_id as repo_id, num_stars '
#             'WITH user_id, collect(repo_id) as repo_id, collect(num_stars) as num_stars, sum(num_stars) as total_stars '
#             'RETURN user_id, repo_id, num_stars, total_stars '
#             'ORDER BY total_stars DESC LIMIT {k}'
#         )

#         result = tx.run(query, {'community': category_id, 
#                                     'start_date': start_date,
#                                     'end_date': end_date,
#                                     'k': k,
#                                    }).data()
        #get all users who pushed to repos with this category in descending order of num stars
        #also get repos of each user
        query = (
            'MATCH (u1:`User`)-[w:`WatchEvent`]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} AND {community} IN r1.categories '            
            'WITH r1, count(w) as num_stars '
            'MATCH ()-[p]-(r1) '
            'WHERE {start_date} <= p.date <= {end_date} '
            "AND (type(p) = 'PushEvent' OR type(p) = 'CommitCommentEvent') "
            'WITH r1, num_stars, sum(p.count) as total_pcommits '
            'MATCH (u2:`User`)-[p]-(r1) '
            'WHERE {start_date} <= p.date <= {end_date} '
            "AND (type(p) = 'PushEvent' OR type(p) = 'CommitCommentEvent') "
            'WITH DISTINCT u2.user_id as user_id, r1.repo_id as repo_id, sum(p.count) * 1.0/ total_pcommits as weight, num_stars '
            'WITH user_id, collect(repo_id) as repo_id, collect(num_stars) as num_stars, sum(num_stars) as total_stars, '
            'sum(num_stars * weight) as score, collect(weight) as weight '
            'RETURN user_id, repo_id, num_stars, total_stars, weight, score '
            'ORDER BY score DESC LIMIT {k}'
        )
        result = tx.run(query, {'community': category, 
                                    'start_date': start_date,
                                    'end_date': end_date,
                                    'k': k,
                                   }).data()
        result_df = pd.DataFrame(result)
        if len(result_df):
            result_df['user_name'] = result_df['user_id'].apply(lambda x: user_id2name[x])
            #make list of repo_ids into one row each. Now it's a network
            network_df = explode(result_df, ['repo_id', 'num_stars'])        
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        create_using = nx.Graph())   
            
            if layout == 'spring':
                pos = nx.spring_layout(G)
            else: 
                pos = nx.random_layout(G)            
            repos = network_df['repo_id'].unique()
            users = network_df['user_id'].unique()
            #all rows with same repo id have same num stars
            repos2stars = network_df.groupby(['repo_id'])[['num_stars']].mean()
        
            for repo_id in repos:
                G.add_node(repo_id, node_type = 'repo', name = repo_id2name[repo_id], stars = repos2stars.loc[repo_id]['num_stars'])
         
            for user_id in users:
                G.add_node(user_id, node_type = 'user', name = user_id2name[user_id])
                
            edge_trace = go.Scatter(
                x=[],
                y=[],
                line=dict(width=0.5,color='#888'),
                hoverinfo='none',
                mode='lines', name = 'Commit/Push Activity')

            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])
                

            repo_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='orange',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Repo', name = 'Repo'
            )

            for node in repos:
                x, y = pos[node]
                repo_trace['x'] += tuple([x])
                repo_trace['y'] += tuple([y])
                text = G.nodes[node]['name']
                text += '<br>Number of stars: '
                text += str(G.nodes[node]['stars'])
                repo_trace['text'] += tuple([text])
            
            user_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='blue',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )
            

            for node in users:
                x, y = pos[node]
                user_trace['x'] += tuple([x])
                user_trace['y'] += tuple([y])
                user_trace['text'] += tuple([G.nodes[node]['name']])
            fig = go.Figure(data=[edge_trace, repo_trace, user_trace],
                                     layout=go.Layout(
                                        titlefont=dict(size=16),
                                        showlegend=True,
                                        hovermode='closest',
                                        margin=dict(b=20,l=5,r=5,t=40),
                                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            return [html.Div("Rising Users for {}".format(category)), generate_table(result_df[['user_name', 'total_stars', 'score']]), dcc.Graph('rising_users_graph', figure = fig)]
    print("Not valid")
    return ["None", None, None]


def compute_score(row, push_weight, star_weight):
    #computes 'score' of a row based on given weights for push and star events
    if row['event'] in ['PushEvent', 'CommitCommentEvent']:
        return push_weight * row['num_edges']
    elif row['event'] == 'WatchEvent':
        return star_weight * row['num_edges']

#OBSOLETE SINCE SWITCHED FROM LABEL PROPAGATION TO MANUAL
def visualize_user_profile_old(user_name, start_date, end_date, push_weight, star_weight):
    if user_name in user_name2id:
        user_id = user_name2id[user_name]
        #get all repos for the user that user pushed or starred to 
        query = (
            'MATCH (u1:`User` {user_id:{user_id}})-[w]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            "AND (type(w) = 'PushEvent' OR type(w) = 'WatchEvent') "
            'RETURN u1.user_id as user_id, u1.user_name as user_name, type(w) as event, w.date as date, w.count as num_edges, r1.repo_id as repo_id, r1.repo_name as repo_name, r1.community as community'
        )
        user_graph = tx.run(query, {'user_id': user_id, 
                                            'start_date': start_date,
                                            'end_date': end_date}).data()
        network_df = pd.DataFrame(user_graph)
        network_df['community'] = network_df['community'].fillna('unknown')
        network_df['community'] = network_df['community'].apply(lambda x: id2community[x])
        if len(network_df):
            #calculate score for each of the community user is involved in based on push and star events. 
            user_profile = network_df.groupby(['community', 'event'])[['num_edges']].sum().reset_index()
            user_profile['score'] = user_profile.apply(compute_score, axis = 1, args = (push_weight, star_weight)) 
            user_profile = user_profile.groupby(['community'])[['score']].sum().reset_index()
            user_profile['proportion'] = user_profile['score'] / user_profile['score'].sum()
            user_profile = user_profile[['community', 'proportion']]
            network_df['score'] = network_df.apply(compute_score, axis = 1, args = (push_weight, star_weight)) 
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        edge_attr = ['date', 'event', 'num_edges'],
                                        create_using = nx.Graph())
            #Make node attributes (node_type, name, community, score) for repos
            G.add_node(user_id, node_type = 'user', name = user_name)
            unique_repos = network_df.groupby(['repo_id', 'repo_name', 'community'])[['score']].sum().reset_index()
            for i, repo in unique_repos.iterrows():
                G.add_node(repo['repo_id'], node_type = 'repo', name = repo['repo_name'], 
                           community = repo['community'], score = repo['score'])    

            pos = nx.spring_layout(G)

            edge_trace = go.Scatter(
                x=[],
                y=[],
#                 text=[],
                line=dict(width=0.5,color='#888'),
#                 hoverinfo='text',
                mode='lines', name = 'Activity')

            event = nx.get_edge_attributes(G, 'event')
            date = nx.get_edge_attributes(G, 'date')
            count = nx.get_edge_attributes(G, 'num_edges')
            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])
#                 edge_trace['text'] += tuple([str(event[edge]) + ' ' + str(date[edge]) + ' ' + str(count[edge])])

            user_node_trace = go.Scatter(
                x=[pos[user_id][0]],
                y=[pos[user_id][1]],
                text=[user_name],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='red',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )
            
            repo_traces = []
            
            
            #For each community, for the repos in the community, start new trace so that they can have different colors
            for i, community in enumerate(unique_repos['community'].unique()):
                repo_trace = go.Scatter(
                    x=[],
                    y=[],
                    text=[],
                    mode='markers',
                    hoverinfo='text',
                    marker=dict(
                        showscale=False,
                        colorscale='YlGnBu',
                        reversescale=True,
                        size=20,
                        line=dict(width=2)),
                    legendgroup = community + ' community Repo', name = community + ' community Repo'
                )

                for node in unique_repos[unique_repos.community == community]['repo_id']:
                    x, y = pos[node]
                    repo_trace['x'] += tuple([x])
                    repo_trace['y'] += tuple([y])
                    repo_trace['text'] += tuple([G.nodes[node]['name'] + '<br> score:' + str(G.nodes[node]['score'])])
                repo_traces.append(repo_trace)

            fig = go.Figure(data=[edge_trace, user_node_trace] + repo_traces,
                         layout=go.Layout(
                            titlefont=dict(size=16),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            
            user_profile = user_profile.sort_values('proportion', ascending = False)
            return html.Div("User Profile for {}".format(user_name)), generate_table(user_profile), dcc.Graph('profile', figure = fig)
        
    return ["None", None, None]    
    
def visualize_user_profile(user_name, start_date, end_date, push_weight, star_weight, layout = 'spring'):
    if user_name in user_name2id:
        user_id = user_name2id[user_name]
        #get all repos for the user that user pushed or starred to 
        query = (
            'MATCH (u1:`User` {user_id:{user_id}})-[w]-(r1:`Repo`) '
            'WHERE {start_date} <= w.date <= {end_date} '
            "AND (type(w) = 'PushEvent' OR type(w) = 'CommitCommentEvent' OR type(w) = 'WatchEvent') "
            'RETURN u1.user_id as user_id, u1.user_name as user_name, type(w) as event, w.date as date, w.count as num_edges, r1.repo_id as repo_id, r1.repo_name as repo_name, r1.categories as community'
        )
        user_graph = tx.run(query, {'user_id': user_id, 
                                            'start_date': start_date,
                                            'end_date': end_date}).data()
        network_df = pd.DataFrame(user_graph)
        network_df['community'] = network_df['community'].apply(lambda d: d if isinstance(d, list) else ['unknown'])
        network_df = network_df.explode('community')
        if len(network_df):
            #calculate score for each of the community user is involved in based on push and star events. 
            user_profile = network_df.groupby(['community', 'event'])[['num_edges']].sum().reset_index()
            user_profile['score'] = user_profile.apply(compute_score, axis = 1, args = (push_weight, star_weight)) 
            user_profile = user_profile.groupby(['community'])[['score']].sum().reset_index()
            user_profile['proportion'] = user_profile['score'] / user_profile['score'].sum()
            user_profile = user_profile[['community', 'proportion']]
            network_df['score'] = network_df.apply(compute_score, axis = 1, args = (push_weight, star_weight)) 
            G = nx.from_pandas_edgelist(network_df, 
                                        source = 'user_id', 
                                        target = 'repo_id', 
                                        edge_attr = ['date', 'event', 'num_edges'],
                                        create_using = nx.Graph())
            #Make node attributes (node_type, name, community, score) for repos
            G.add_node(user_id, node_type = 'user', name = user_name)
            unique_repos = network_df.groupby(['repo_id', 'repo_name'])[['score']].count().reset_index()
            for i, repo in unique_repos.iterrows():
                G.add_node(repo['repo_id'], node_type = 'repo', name = repo['repo_name'])    

            if layout == 'spring':
                pos = nx.spring_layout(G)
            else: 
                pos = nx.random_layout(G)
                
            edge_trace = go.Scatter(
                x=[],
                y=[],
                line=dict(width=0.5,color='#888'),
                mode='lines', name = 'Activity')

            for edge in G.edges():
                x0, y0 = pos[edge[0]]
                x1, y1 = pos[edge[1]]
                edge_trace['x'] += tuple([x0, x1, None])
                edge_trace['y'] += tuple([y0, y1, None])

            user_node_trace = go.Scatter(
                x=[pos[user_id][0]],
                y=[pos[user_id][1]],
                text=[user_name],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    color='red',
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'User', name = 'User'
            )
            

            repo_trace = go.Scatter(
                x=[],
                y=[],
                text=[],
                mode='markers',
                hoverinfo='text',
                marker=dict(
                    showscale=False,
                    colorscale='YlGnBu',
                    reversescale=True,
                    size=20,
                    line=dict(width=2)),
                legendgroup = 'Repo', name = 'Repo'
            )

            for i, repo in unique_repos.iterrows():
                repo_id = repo['repo_id']
                x, y = pos[repo_id]
                repo_trace['x'] += tuple([x])
                repo_trace['y'] += tuple([y])
                #get rows corresponding to repo id
                relevant_rows = network_df[network_df.repo_id == repo_id]
                #calculate score of repo for each community
                scores = relevant_rows.groupby(['community'])[['score']].sum().reset_index()
                text_str = repo['repo_name']
                for j, row in scores.iterrows():
                    text_str += '<br>' + row['community'] + ' score: ' + str(row['score'])
                repo_trace['text'] += tuple([text_str])

            fig = go.Figure(data=[edge_trace, user_node_trace, repo_trace],
                         layout=go.Layout(
                            titlefont=dict(size=16),
                            showlegend=True,
                            hovermode='closest',
                            margin=dict(b=20,l=5,r=5,t=40),
                            xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                            yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
            
            user_profile = user_profile.sort_values('proportion', ascending = False)
            return html.Div("User Profile for {}".format(user_name)), generate_table(user_profile), dcc.Graph('profile', figure = fig)
        
    return ["None", None, None]
    
def generate_table(dataframe):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(len(dataframe))]
    )

