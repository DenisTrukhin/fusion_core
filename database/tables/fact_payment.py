from .base import CHTable


class Fact_Payment(CHTable):


	@property
	def definition(self):
		query = '''
			CREATE TABLE IF NOT EXISTS {db}.fact_payment
			(
				`ts` UInt32,
				`user_id` UInt32,
				`USD` UInt32
			)
			ENGINE = MergeTree()
			ORDER BY `ts`
			'''.format(db=self.db_name)
		return query