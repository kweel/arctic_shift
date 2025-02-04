import sys
version = sys.version_info
if version.major < 3 or (version.major == 3 and version.minor < 10):
	raise RuntimeError("This script requires Python 3.10 or higher")
import os
from typing import Iterable
from datetime import datetime

from fileStreams import getFileJsonStream
from utils import FileProgressLog
import csv


fileOrFolderPath = r"/Users/kevinlee/r4r-LL/json/r_LocalLLaMA_posts.jsonl"
recursive = False

def processFile(path: str):
	print(f"Processing file {path}")
	with open(path, "rb") as f:
		jsonStream = getFileJsonStream(path, f)
		if jsonStream is None:
			print(f"Skipping unknown file {path}")
			return
		progressLog = FileProgressLog(path, f)
		
		# Define the CSV file path
		csvFilePath = "/Users/kevinlee/Desktop/posts.csv"
		with open(csvFilePath, mode='w', newline='') as csvFile:
			csvWriter = csv.writer(csvFile)
			
			# Write the header
			header = ["author", "removal_type", "delete_status", "removal_reason", "crosspost_parent", "crosspost_parent_subreddit", "id", "num_comments", "title", "body", "created", "score", "flair", "link"]
			csvWriter.writerow(header)
			
			for row in jsonStream:
				progressLog.onRow()
		
				author = row["author"]
				removal_type = row.get("_meta", {}).get("removal_type", None)
				was_deleted_later = row.get("_meta", {}).get("was_deleted_later", None)
				removal_reason = row.get("removal_reason", None)
				# approved = row.get("approved", None)
				# approved_by = row.get("approved_by", None)
				crosspost_parent = row.get("crosspost_parent", None)
				# crosspost_parent_subreddit = row.get("crosspost_parent_subreddit", [{}]).get("subreddit", None)
				if "crosspost_parent_subreddit" in row and isinstance(row["crosspost_parent_subreddit"], list):
					crosspost_parent_subreddit = row["crosspost_parent_subreddit"][0].get("subreddit", None) if row["crosspost_parent_subreddit"] else None
				else:
					crosspost_parent_subreddit = None
				id = row["id"]
				num_comments = row["num_comments"]
				title = row["title"]
				body = row["selftext"]
				created = datetime.fromtimestamp(row["created_utc"])
				score = row["score"]
				flair = row["link_flair_text"]
				link = row["permalink"]
				# removed = row.get("removed", None)
				# Write the row to the CSV file
				csvWriter.writerow([author, removal_type, was_deleted_later, removal_reason, crosspost_parent, crosspost_parent_subreddit, id, num_comments, title, body, created, score, flair, link])
				
		progressLog.logProgress("\n")
	

def processFolder(path: str):
	fileIterator: Iterable[str]
	if recursive:
		def recursiveFileIterator():
			for root, dirs, files in os.walk(path):
				for file in files:
					yield os.path.join(root, file)
		fileIterator = recursiveFileIterator()
	else:
		fileIterator = os.listdir(path)
		fileIterator = (os.path.join(path, file) for file in fileIterator)
	
	for i, file in enumerate(fileIterator):
		print(f"Processing file {i+1: 3} {file}")
		processFile(file)

def main():
	if os.path.isdir(fileOrFolderPath):
		processFolder(fileOrFolderPath)
	else:
		processFile(fileOrFolderPath)
	
	print("Done :>")

if __name__ == "__main__":
	main()