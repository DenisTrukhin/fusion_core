import time


class Procedure:


	def __init__(self, db):
		self.db = db


	def __call__(self, options):
		raise NotImplementedError


class timer:


	def __init__(self, msg):
		self.start = time.time()
		print('Start {}'.format(msg))


	def __enter__(self):
		return self


	def __exit__(self, *args):
		print('Elapsed time: {} seconds'.format(time.time() - self.start))