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
from dashboard_variables import *


app = dash.Dash()
app.config['suppress_callback_exceptions'] = True

app.layout = html.Div(children = [
	html.H1(children = 'Github Explorer'),
    html.Div(children = 'Graph Layout:'), 
    dcc.Dropdown(id = 'graph-layout-input',
        options=[          
            {'label': 'Spring', 'value': 'spring'},
            {'label': 'Random', 'value': 'random'},
        ],
        value = 'spring'
    ),    
    html.Br(),
    html.Div(children = 'Explore:'), 
    dcc.Dropdown(id = 'explore-input',
        options=[          
            {'label': 'Recommendations', 'value': 'recommendations'},
            {'label': 'Rising Repos', 'value': 'rising_repos'},
            {'label': 'Rising Users', 'value': 'rising'},
            {'label': 'User Profile', 'value': 'profile'},
        ],
        value = 'recommendations'
    ),
    html.Br(),
    html.Div(id = 'user-input') 
])



@app.callback(
    Output(component_id = 'user-input', component_property = 'children'),
    [Input(component_id = 'explore-input', component_property = 'value')]
)
def update_explore_text(explore_type):
    #change UI based on exploration type
    rtn_list = []
    rtn_list.append(html.Div(explore_text_options[explore_type]))
    if explore_type == 'recommendations':
        rtn_list.append(dcc.Dropdown(id = 'recommend-type-input', options = explore_dropdown_options[explore_type]))
        rtn_list += [
            html.Div(children = 'Github user name or repo name:', style={'marginTop': '0.5em'}),
            dcc.Input(id = 'recommend-username-input', type = 'text'),
            html.Div(children = 'Max number of recommendations: ', style={'marginTop': '0.5em'}),
            dcc.Input(id = 'recommend-k-input', value = 10, type = 'text'),
            html.Div(children = 'Activity range:', style={'marginTop': '0.5em'}),
            html.Div(dcc.DatePickerRange(
                id='recommend-date-picker-range',
                min_date_allowed=dt(2019, 10, 1),
                max_date_allowed=dt(2019, 11, 1),
                initial_visible_month=dt(2019, 10, 1),
                start_date='2019-10-01',
                end_date='2019-10-31'
            )),
            html.Div(id = 'recommend-output-username', style={'marginTop': '0.5em'}),
            html.Div(id = 'recommend-output-df'),
            html.Div(id = 'recommend-output-graph')        
        ]
    
    elif explore_type == 'rising_repos':
        rtn_list.append(dcc.Dropdown(id = 'rising-repos-type-input', options = explore_dropdown_options[explore_type]))
        rtn_list += [
            html.Div(children = 'Max number of rising repos: ', style={'marginTop': '0.5em'}),
            dcc.Input(id = 'rising-repos-k-input', value = 10, type = 'text'),
            html.Div(children = 'Activity range:', style={'marginTop': '0.5em'}),
            html.Div(dcc.DatePickerRange(
                id='rising-repos-date-picker-range',
                min_date_allowed=dt(2019, 10, 1),
                max_date_allowed=dt(2019, 11, 1),
                initial_visible_month=dt(2019, 10, 1),
                start_date='2019-10-01',
                end_date='2019-10-31'
            )),    
            html.Div(id = 'rising-repos-output-text', style={'marginTop': '0.5em'}),
            html.Div(id = 'rising-repos-output-df'),
            html.Div(id = 'rising-repos-output-graph')    
        ]
    
    elif explore_type == 'rising':
        rtn_list.append(dcc.Dropdown(id = 'rising-type-input', options = explore_dropdown_options[explore_type]))
        rtn_list += [
            html.Div(children = 'Max number of rising users: ', style={'marginTop': '0.5em'}),
            dcc.Input(id = 'rising-k-input', value = 10, type = 'text'),
            html.Div(children = 'Activity range:', style={'marginTop': '0.5em'}),
            html.Div(dcc.DatePickerRange(
                id='rising-date-picker-range',
                min_date_allowed=dt(2019, 10, 1),
                max_date_allowed=dt(2019, 11, 1),
                initial_visible_month=dt(2019, 10, 1),
                start_date='2019-10-01',
                end_date='2019-10-31'
            )),    
            html.Div(id = 'rising-output-text', style={'marginTop': '0.5em'}),
            html.Div(id = 'rising-output-df'),
            html.Div(id = 'rising-output-graph')    
        ]
    elif explore_type == 'profile':
        rtn_list += [
            dcc.Input(id = 'profile-username-input', type = 'text'),
            html.Div(children = 'Commit/Push Weight:', style={'marginTop': '0.5em'}),            
            dcc.Input(id = 'profile-push-weight', type = 'text', value = 1),
            html.Div(children = 'Star Weight:', style={'marginTop': '0.5em'}),            
            dcc.Input(id = 'profile-star-weight', type = 'text', value = 12),
            html.Div(children = 'Activity range:', style={'marginTop': '0.5em'}),
            html.Div(dcc.DatePickerRange(
                id='profile-date-picker-range',
                min_date_allowed=dt(2019, 10, 1),
                max_date_allowed=dt(2019, 11, 1),
                initial_visible_month=dt(2019, 10, 1),
                start_date='2019-10-01',
                end_date='2019-10-31'
            )),
            html.Div(id = 'profile-output-username', style={'marginTop': '0.5em'}),
            html.Div(id = 'profile-output-df'),
            html.Div(id = 'profile-output-graph')   
        ]
    return rtn_list

@app.callback(
    [Output(component_id = 'recommend-output-username', component_property = 'children'),
    Output(component_id = 'recommend-output-df', component_property = 'children'),
    Output(component_id = 'recommend-output-graph', component_property = 'children')],
    [Input(component_id = 'recommend-type-input', component_property = 'value'),
    Input(component_id = 'recommend-username-input', component_property = 'value'),
    Input(component_id = 'recommend-k-input', component_property = 'value'),
    Input(component_id = 'recommend-date-picker-range', component_property = 'start_date'),
    Input(component_id = 'recommend-date-picker-range', component_property = 'end_date'),
    Input(component_id = 'graph-layout-input', component_property = 'value')]
)
def update_recommender_visualization(recommend_type, name, k, start_date, end_date,
                                           layout, threshold = 0):
    print("updating recommender")
    try:
        k = int(k)
    except:
        k = 10 
    try:
        result = [None, None, None]
        if recommend_type == 'ru':
            result = visualize_repo_recommendations_to_user(name, k, start_date, end_date,
                                               layout, threshold = 0)
        elif recommend_type == 'uu':
            result = visualize_user_recommendations_to_user(name, k, start_date, end_date, layout, threshold = 0)
        elif recommend_type == 'rr':
            result = visualize_repo_recommendations_to_repo(name, k, start_date, end_date, layout, threshold = 0)
        return result
    except Exception as e:
        print(e)
        return ["Error", None, None]
    return [None, None, None]
    
@app.callback(
    [Output(component_id = 'rising-output-text', component_property = 'children'),
    Output(component_id = 'rising-output-df', component_property = 'children'),
    Output(component_id = 'rising-output-graph', component_property = 'children')],
    [Input(component_id = 'rising-type-input', component_property = 'value'),
    Input(component_id = 'rising-k-input', component_property = 'value'),
    Input(component_id = 'rising-date-picker-range', component_property = 'start_date'),
    Input(component_id = 'rising-date-picker-range', component_property = 'end_date'),
    Input(component_id = 'graph-layout-input', component_property = 'value')]
)
def update_rising_star_visualization(category, k, start_date, end_date, layout):
    print("updating rising star")
    try:
        k = int(k)
    except:
        k = 10 
    try:
        result = visualize_rising_stars(category, k, start_date, end_date, layout)
        return result
    except Exception as e:
        print(e)
        return ["Error", None, None]
    
    
@app.callback(
    [Output(component_id = 'rising-repos-output-text', component_property = 'children'),
    Output(component_id = 'rising-repos-output-df', component_property = 'children'),
    Output(component_id = 'rising-repos-output-graph', component_property = 'children')],
    [Input(component_id = 'rising-repos-type-input', component_property = 'value'),
    Input(component_id = 'rising-repos-k-input', component_property = 'value'),
    Input(component_id = 'rising-repos-date-picker-range', component_property = 'start_date'),
    Input(component_id = 'rising-repos-date-picker-range', component_property = 'end_date'),
    Input(component_id = 'graph-layout-input', component_property = 'value')]
)
def update_rising_repos_visualization(category, k, start_date, end_date, layout):
    print("updating rising repos")
    try:
        k = int(k)
    except:
        k = 10 
    try:
        result = visualize_rising_stars_repos(category, k, start_date, end_date, layout)
        return result
    except Exception as e:
        print(e)
        return ["Error", None, None]
    
@app.callback(
    [Output(component_id = 'profile-output-username', component_property = 'children'),
    Output(component_id = 'profile-output-df', component_property = 'children'),
    Output(component_id = 'profile-output-graph', component_property = 'children')],
    [Input(component_id = 'profile-username-input', component_property = 'value'),
    Input(component_id = 'profile-date-picker-range', component_property = 'start_date'),
    Input(component_id = 'profile-date-picker-range', component_property = 'end_date'), 
    Input(component_id = 'profile-push-weight', component_property = 'value'),
    Input(component_id = 'profile-star-weight', component_property = 'value'),
    Input(component_id = 'graph-layout-input', component_property = 'value')]
)
def update_profile_visualization(name, start_date, end_date, push_weight, star_weight, layout):
    print("updating profile")
    try:
        push_weight = float(push_weight)
        star_weight = float(star_weight)
        # print(name, start_date, end_date, push_weight, star_weight, layout)
        result = visualize_user_profile(name, start_date, end_date, push_weight, star_weight, layout)
        return result
    except Exception as e:
        print(e)
        return ["Error", None, None]

if __name__ == '__main__':
	app.run_server(debug = True)