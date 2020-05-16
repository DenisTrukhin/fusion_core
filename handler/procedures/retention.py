from tabulate import tabulate
from .base import Procedure, timer


class Retention(Procedure):


	_query = '''
		SELECT 
		dateDiff('day', toDateTime({db}.fact_reg.ts), toDateTime({db}.fact_login.ts)) AS day, 
		countDistinct(user_id) AS retention
		FROM {db}.fact_reg
		LEFT JOIN {db}.fact_login ON {db}.fact_reg.user_id = {db}.fact_login.user_id
		WHERE ({db}.fact_reg.ts >= toUnixTimestamp('{period_start}')) 
			AND ({db}.fact_login.ts <= toUnixTimestamp('{period_end}')) 
			AND match({db}.fact_reg.ref, '{pattern}')
		GROUP BY day
		ORDER BY day
		'''


	def __call__(self, options):
		if not options.start or not options.end:
			raise TypeError('Wrong period')
		with timer('retention procedure') as t:
			result = self.db.client.execute(
				self._query.format(
					db=self.db.name,
					period_start=options.start,
					period_end=options.end,
					pattern=options.ref
				)
			)
		return tabulate(result, headers=('day', 'retention'), tablefmt='orgtbl')