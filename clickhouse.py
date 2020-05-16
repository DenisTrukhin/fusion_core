import argparse		
from database import Database
from handler import Handler


def parse_options():
	parser = argparse.ArgumentParser()
	parser.add_argument('-u', '--user', type=str, default='default', help='clickhouse user')
	parser.add_argument('-p', '--password', type=str, default='', help='clickhouse password')
	parser.add_argument('--host', type=str, default='localhost', help='clickhouse-server host')
	parser.add_argument('--port', type=int, default=9000, help='clickhouse-server port')
	parser.add_argument('--db', type=str, default='test', help='database name')
	parser.add_argument('-n', '--number', type=int, default=1000000, help='number of fake records to be inserted')
	parser.add_argument('-o', '--operation', type=str, help='operation to be performed')
	parser.add_argument('-s', '--start', type=str, help='period start date')
	parser.add_argument('-e', '--end', type=str, help='period end date')
	parser.add_argument('-r', '--ref', type=str, default='', help='source template')
	return parser.parse_args()


def process(options):
	db = Database(
		user=options.user,
		password=options.password, 
		host=options.host,
		port=options.port,
		db_name=options.db
	)
	
	handler = Handler(db)
	
	if options.operation:
		try:
			result = handler.execute(options)
			print(result)
		except KeyError:
			print("Unknown operation '{}'".format(options.operation))


if __name__ == '__main__':
	# set_start_method('spawn')
	options = parse_options()
	process(options)