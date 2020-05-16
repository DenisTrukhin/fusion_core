import os
import numpy as np
import datetime
import functools
from multiprocessing import get_context
from .base import Procedure, timer



class Insert(Procedure):
	

	_refs_and_probabilities = {
		'ads1': 0.45,
		'ads2': 0.15,
		'social1': 0.25,
		'social2': 0.08, 
		'search': 0.07
	}


	def __init__(self, db):
		super().__init__(db)
		self.start_datetime = datetime.datetime.strptime('04-05-2020 00:00:00', '%d-%m-%Y %H:%M:%S')
		self.end_datetime = self.start_datetime + datetime.timedelta(days=7)


	def __call__(self, options):
		self.db.create_database()
		self.db.create_tables()
		self.insert(options.number)


	@staticmethod
	def _get_exp_var(lmb):
		return -np.log(1. - (1. - np.exp(-lmb)) * np.random.uniform()) / lmb


	@staticmethod
	def _get_random_datetime(start_datetime, end_datetime, coef):
		delta = int((end_datetime - start_datetime).total_seconds())
		return start_datetime + datetime.timedelta(seconds=int(coef*delta))


	def _get_user_data(self, n_users):
		max_payment = 1000
		sigma = 5.
		for i in range(n_users):
			ref = np.random.choice(
				list(self._refs_and_probabilities.keys()), 
				p=list(self._refs_and_probabilities.values())
			)
			
			reg_datetime = Insert._get_random_datetime(self.start_datetime, self.end_datetime, np.random.uniform())
			
			login_datetimes = [reg_datetime] + [
				Insert._get_random_datetime(
					reg_datetime, 
					self.end_datetime, 
					self._get_exp_var(3.)
				) for i in range(np.random.randint(30))
			]
			
			payments_datetimes = [
				min(dt + datetime.timedelta(seconds=np.random.randint(30, 600)), self.end_datetime) \
				for dt in login_datetimes if np.random.uniform() < 0.3
			]
			avg_person_payment = self._get_exp_var(5.) * max_payment
			payments = zip([np.abs(np.random.normal(avg_person_payment, sigma)) for i in range(len(login_datetimes))], login_datetimes)
			yield ref, reg_datetime, login_datetimes, payments
	

	def insert_batch(self, args):
		start_point = args['start_point']
		batch_size = args['batch_size']
		datetime_fmt = '%Y-%m-%d %H:%M:%S'
		
		ids = (i for i in range(start_point, start_point + batch_size))
		
		insert_fact_reg = 'INSERT INTO {}.fact_reg (`user_id`, `ref`, `ts`) VALUES '.format(self.db.name)
		insert_fact_log = 'INSERT INTO {}.fact_login (`user_id`, `ts`) VALUES '.format(self.db.name)
		insert_fact_pay = 'INSERT INTO {}.fact_payment (`user_id`, `USD`, `ts`) VALUES '.format(self.db.name)
		
		for i, user_data in enumerate(self._get_user_data(batch_size)):
			ref, reg_datetime, login_datetimes, payments = user_data
			user_id = next(ids)
			sep = ',' if i < batch_size - 1 else ';'
			insert_fact_reg += "({}, '{}', toUnixTimestamp('{}'))".format(
				user_id, ref, reg_datetime.strftime(datetime_fmt)
			) + sep
			insert_fact_log += ','.join([
				"({}, toUnixTimestamp('{}'))".format(user_id, ts.strftime(datetime_fmt)) for ts in login_datetimes
			]) + sep
			insert_fact_pay += ','.join([
				"({}, {}, toUnixTimestamp('{}'))".format(user_id, usd, ts.strftime(datetime_fmt)) for usd, ts in payments
			]) + sep
		return insert_fact_reg, insert_fact_log, insert_fact_pay


	def insert(self, n_users):
		cpus = os.cpu_count()
		batch_size = n_users // cpus
		args = [{'start_point': start_point, 'batch_size': batch_size} for start_point in range(0, n_users, batch_size)]
		with timer('data generation') as t:
			with get_context('spawn').Pool(cpus) as pool:
				all_queries = pool.map(self.insert_batch, args)
				pool.close()
				pool.join()
		with timer('inserting') as t:
			for batch_queries in all_queries:
				for query in batch_queries:
					self.db.client.execute(query)
