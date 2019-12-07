import sys
from sqlengine import query_processing

if __name__ == '__main__':
	if len(sys.argv) < 2:
		print "You haven't given a query to process. Please run again."

	else:
		process = query_processing(sys.argv[1])