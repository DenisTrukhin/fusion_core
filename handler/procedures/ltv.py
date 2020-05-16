from tabulate import tabulate
from .base import Procedure, timer


class LTV(Procedure):


	_query = '''
		SELECT avg(USD)
		FROM {db}.fact_payment
		LEFT JOIN {db}.fact_reg ON {db}.fact_payment.user_id = {db}.fact_reg.user_id
		WHERE (ts >= toUnixTimestamp('{period_start}')) 
			AND (ts <= toUnixTimestamp('{period_end}')) 
			AND match(ref, '{pattern}')
		'''


	def __call__(self, options):
		if not options.start or not options.end:
			raise TypeError('Wrong period')
		with timer('ltv procedure') as t:
			result = self.db.client.execute(
				self._query.format(
					db=self.db.name,
					period_start=options.start,
					period_end=options.end,
					pattern=options.ref
				)
			)
		return tabulate(result, headers=('ltv',), tablefmt='orgtbl')