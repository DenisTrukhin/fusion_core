from .base import CHTable


class Fact_Reg(CHTable):


	@property
	def definition(self):
		query = '''
			CREATE TABLE IF NOT EXISTS {db}.fact_reg
			(
				`ts` UInt32,
				`user_id` UInt32,
				`ref` String
			)
			ENGINE = MergeTree()
			ORDER BY `ts`
			'''.format(db=self.db_name)
		return query
