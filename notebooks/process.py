import sys
import os
import pandas as pd
import shutil


def process(date1, date2, preprocess_directory, process_directory): 
	"""
	Process preprocessed files for a date, date1, such that each repository gets stars gained in future date, date2.
	Preprocessed files should be in preprocess_directory with each date being its own directory.
	Saves to process_directory a date1 directory with processed repofile + same user csv and triplets csv.
	"""
	repo1 = pd.read_csv(preprocess_directory + '/' + date1 + '/' + 'repos.csv')
	repo2 = pd.read_csv(preprocess_directory + '/' + date2 + '/' + 'repos.csv')
	repo2 = repo2.rename(columns = {'WatchEvent': 'WatchEventFuture'})
	new_repo1 = repo1.merge(repo2[['repo_id', 'WatchEventFuture']], on = ['repo_id'], how = 'left')
	new_repo1['WatchEventFuture'] = new_repo1['WatchEventFuture'].fillna(0)
	if not os.path.exists(process_directory):
		os.mkdir(process_directory)
	if not os.path.exists(preprocess_directory + '/' + date1):
		os.mkdir(preprocess_directory + '/' + date1)
	new_repo1.to_csv(preprocess_directory + '/' + date1 + '/' + 'repos_' + date2 + '.csv', index = False)
	shutil.copy2(preprocess_directory + '/' + date1 + '/' + 'users.csv', process_directory + '/' + date1 + '/' + 'users.csv')
	shutil.copy2(preprocess_directory + '/' + date1 + '/' + 'triplets.csv', process_directory + '/' + date1 + '/' + 'triplets.csv')

def main(argv):
	process(argv[0], argv[1], argv[2], argv[3])

if __name__ == '__main__':
	main(sys.argv[1:])