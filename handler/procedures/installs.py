from tabulate import tabulate
from .base import Procedure, timer


class Installs(Procedure):


	_query = '''
		SELECT count()
		FROM {db}.fact_reg
		WHERE (ts >= toUnixTimestamp('{period_start}')) 
			AND (ts <= toUnixTimestamp('{period_end}')) 
			AND match(ref, '{pattern}')
		'''


	def __call__(self, options):
		if not options.start or not options.end:
			raise TypeError('Wrong period')
		with timer('installs procedure') as t:
			result = self.db.client.execute(
				self._query.format(
					db=self.db.name,
					period_start=options.start,
					period_end=options.end,
					pattern=options.ref
				)
			)
		return tabulate(result, headers=('installs',), tablefmt='orgtbl')