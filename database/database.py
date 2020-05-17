import sys
import inspect
from clickhouse_driver import Client
from clickhouse_driver.errors import ServerException
from .tables import *


class Database:


	def __init__(self, user='default', password='', host='localhost', port=9000, db_name=None):
		self.client = Client.from_url('clickhouse://{}:{}@{}:{}'.format(user, password, host, port))
		self._test_connection()
		self.name = db_name
		self._create_database()
		self._create_tables()


	def _test_connection(self):
		try:
			self.client.execute('SELECT now()')
		except ServerException as e:
			auth_failed_code = 516
			if e.args[-1] == auth_failed_code:
				raise asdfasdasd



	def _create_database(self):
		query = 'CREATE DATABASE IF NOT EXISTS {}'.format(self.name)
		self.client.execute(query)


	def _create_tables(self):
		tables_module = sys.modules.get('database.tables')
		if not tables_module:
			raise ModuleNotFoundError('The tables module has been lost')
		tables = [
			member(self.client, self.name) for _, member in inspect.getmembers(tables_module) \
			if inspect.isclass(member) and not '.base' in member.__module__
		]
		for table in tables:
			self.client.execute(table.definition)