import pickle 

process_directory = '../data/processed/'

#note technically not community2id anymore since just a list instead of dictionary. 
with open(process_directory + '/' + 'all_categories.pkl', 'rb') as f:
# with open(process_directory + '/' + 'community2id.pkl', 'rb') as f:
    community2id = pickle.load(f)
    community2id = sorted(list(community2id))


explore_text_options = {}
explore_text_options['recommendations'] = 'Recommend:'
explore_text_options['rising_repos'] = 'Rising repos for community:'
explore_text_options['rising'] = 'Rising users for community:'
explore_text_options['profile'] = 'Github username:'



explore_dropdown_options = {}
explore_dropdown_options['recommendations'] = [
                                        {'label': 'Users to User', 'value': 'uu'},
                                        {'label': 'Repos to User', 'value': 'ru'},
                                        {'label': 'Repos by Repo', 'value': 'rr'},
                                      ]


explore_dropdown_options['rising'] = [
                               {'label': key, 'value': key} for key in community2id
                             ]

explore_dropdown_options['rising_repos'] = explore_dropdown_options['rising']