import sys
import inspect
from .procedures import *


class Handler:

	
	def __init__(self, db):
		self.db = db
		self._executors = dict()
		self._register_executors()


	def _add_executor(self, name, executor):
		self._executors[name] = executor


	def _register_executors(self):
		modules = [
			(name, module) for name, module in sys.modules.items() \
			if '.procedures' in name and not '.base' in name
		]
		for _, module in modules:
			for name, executor in inspect.getmembers(module):
				if inspect.isclass(executor) and not '.base' in executor.__module__:
					self._add_executor(name.lower(), executor(self.db))


	def execute(self, options):
		if options.operation.lower() in self._executors.keys():
			executor = self._executors[options.operation.lower()]
			return executor(options)
		raise KeyError('Wrong executor name')
