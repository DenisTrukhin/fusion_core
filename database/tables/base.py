class CHTable:


	@property
	def definition(self):
		raise NotImplementedError


	def __init__(self, client, db_name):
		self.client = client
		self.db_name = db_name


	def create(self):
		self.client.execute(self.definition)