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


fileOrFolderPath = r"/Users/kevinlee/r4r-LL/json/r_LocalLLaMA_comments.jsonl"
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
		csvFilePath = "/Users/kevinlee/Desktop/comments.csv"
		with open(csvFilePath, mode='w', newline='') as csvFile:
			csvWriter = csv.writer(csvFile)
			
			# Write the header
			header = ["author", "removal_type", "was_deleted_later", "id", "parent_id", "created", "removal_reason", "score", "flair", "link", "comment"]
			csvWriter.writerow(header)
			
			for row in jsonStream:
				progressLog.onRow()
		
				author = row["author"]
				removal_type = row.get("_meta", {}).get("removal_type", None)
				was_deleted_later = row.get("_meta", {}).get("was_deleted_later", None)
				
				# approved = row.get("approved", None)
				# approved_by = row.get("approved_by", None)
				id = row["id"]
				parent_id = row["parent_id"]
				created = datetime.fromtimestamp(row["created_utc"])
				removal_reason = row.get("removal_reason", None)
				# mod_note = row.get("mod_note", None)
				# mod_reason_by = row.get("mod_reason_by", None)
				# mod_reason_title = row.get("mod_reason_title", None)
				score = row["score"]
				flair = row["author_flair_text"]
				link = row["permalink"]
				comment = row["body"]
				# removed = row.get("removed", None)
				# Write the row to the CSV file
				csvWriter.writerow([author, removal_type, was_deleted_later, id, parent_id, created, removal_reason, score, flair, link, comment])
				
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