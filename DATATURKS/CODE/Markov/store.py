import abc
import collections
import sqlite3


class BaseStore(object, metaclass=abc.ABCMeta):
	def add_one(self, ngram):
		self.add_many((ngram,))

	@abc.abstractmethod
	def add_many(self, ngrams):
		pass

	@abc.abstractmethod
	def get_ngram_values(self, word_1, word_2):
		pass

	@abc.abstractmethod
	def count(self):
		pass

	@abc.abstractmethod
	def trim(self, target_count, min_probability_count=2):
		pass


class SQLiteStore(BaseStore):
	def __init__(self, path=':memory:', wal=False):
		self.connection = sqlite3.connect(path)
		if wal:
			self.connection.execute('PRAGMA journal_mode=WAL')
		self.connection.execute('PRAGMA synchronous=NORMAL')

	def add_many(self, ngrams):
		raise NotImplementedError("Subclass must implement abstract method")
		
	def get_ngram_values(self, word_1, word_2):
		raise NotImplementedError("Subclass must implement abstract method")
		
	def count(self):
		raise NotImplementedError("Subclass must implement abstract method")
		
	def trim(self, target_count, min_probability_count=2):
		raise NotImplementedError("Subclass must implement abstract method")
	
	def count(self):
		query = self.connection.execute(
			'''SELECT COUNT(1) FROM markov_model LIMIT 1'''
		)

		for row in query:
			return row[0]

	def trim(self, target_count, min_probability_count=2):
		num_rows = self.count()
		limit = max(0, num_rows - target_count)

		with self.connection:
			self.connection.execute(
				'''DELETE FROM markov_model WHERE count < ? LIMIT ?''',
				[min_probability_count, limit]
			)


class Bigram(SQLiteStore):
	def __init__(self, path=':memory:', wal=False):
		super().__init__(path, wal)
		self.connection.execute(
			'''CREATE TABLE IF NOT EXISTS
			markov_model
			(
			word_1 TEXT NOT NULL,
			word_2 TEXT NOT NULL,
			count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (word_1, word_2)
			)
			''')

	def add_many(self, bigrams, count = None):
		bigrams = [(int(bigram[0]), bigram[1] or '',bigram[2] or '')
					for bigram in bigrams]

		with self.connection:
			self.connection.executemany(
				'''INSERT OR IGNORE INTO markov_model
				(word_1, word_2) VALUES (?, ?)
				''',
				list(map(lambda x: x[1:], bigrams))
			)
			self.connection.executemany(
				'''UPDATE markov_model
				SET count = ?
				WHERE word_1 = ? AND word_2 = ?
				''',
				bigrams
			)

	def get_ngram_values(self, word_1):
		query = self.connection.execute(
			'''SELECT word_2, count FROM markov_model
			WHERE word_1 = ?
			ORDER BY count DESC LIMIT 1000
			''',
			(word_1 or '')
		)

		value_dict = collections.OrderedDict()

		for row in query:
			value_dict[row[0] or None] = row[1]

		if not value_dict:
			raise KeyError()

		return value_dict
			
class Trigram(SQLiteStore):
	
	def __init__(self, path=':memory:', wal=False):
		super().__init__(path, wal)
		self.connection.execute(
			'''CREATE TABLE IF NOT EXISTS
			markov_model
			(
			word_1 TEXT NOT NULL,
			word_2 TEXT NOT NULL,
			word_3 TEXT NOT NULL,
			count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (word_1, word_2, word_3)
			)
			''')

	def add_many(self, trigrams):
		trigrams = [(int(trigram[0]), trigram[1] or '', trigram[2] or '',trigram[3] or '')
					for trigram in trigrams]
		# print(trigrams)
		with self.connection:
			self.connection.executemany(
				'''INSERT OR IGNORE INTO markov_model
				(word_1, word_2, word_3) VALUES (?, ?, ?)
				''',
				list(map(lambda x: x[1:], trigrams))
			)
			self.connection.executemany(
				'''UPDATE markov_model
				SET count = ?
				WHERE word_1 = ? AND word_2 = ? AND word_3 = ?
				''',
				trigrams
			)

	def get_ngram_values(self, word_1, word_2):
		query = self.connection.execute(
			'''SELECT word_3, count FROM markov_model
			WHERE word_1 = ? AND word_2 = ?
			ORDER BY count DESC LIMIT 1000
			''',
			(word_1 or '', word_2 or '')
		)

		value_dict = collections.OrderedDict()
		# print('call')
		for row in query:
			value_dict[row[0] or None] = row[1]

		if not value_dict:
			raise KeyError()
		
		return value_dict

class Fourgram(SQLiteStore):
	def __init__(self, path=':memory:', wal=False):
		super().__init__(path, wal)
		self.connection.execute(
			'''CREATE TABLE IF NOT EXISTS
			markov_model
			(
			word_1 TEXT NOT NULL,
			word_2 TEXT NOT NULL,
			word_3 TEXT NOT NULL,
			word_4 TEXT NOT NULL,
			count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (word_1, word_2, word_3, word_4)
			)
			''')

	def add_many(self, fourgrams, count = None):
		fourgrams = [(int(fourgram[0]) , fourgram[1] or '', fourgram[2] or '', fourgram[3] or '', fourgram[4] or '')
					for fourgram in fourgrams]

		with self.connection:
			self.connection.executemany(
				'''INSERT OR IGNORE INTO markov_model
				(word_1, word_2, word_3, word_4) VALUES (?, ?, ?, ?)
				''',
				list(map(lambda x: x[1:], fourgrams))
			)
			self.connection.executemany(
				'''UPDATE markov_model
				SET count = ?
				WHERE word_1 = ? AND word_2 = ? AND word_3 = ? AND word_4 = ?
				''',
				fourgrams
			)

	def get_ngram_values(self, word_1, word_2, word_3):
		query = self.connection.execute(
			'''SELECT word_4, count FROM markov_model
			WHERE word_1 = ? AND word_2 = ? AND word_3 = ?
			ORDER BY count DESC LIMIT 1000
			''',
			(word_1 or '', word_2 or '', word_3 or '')
		)

		value_dict = collections.OrderedDict()

		for row in query:
			value_dict[row[0] or None] = row[1]

		if not value_dict:
			raise KeyError()

		return value_dict
	
class Fivegram(SQLiteStore):
	def __init__(self, path=':memory:', wal=False):
		super().__init__(path, wal)
		self.connection.execute(
			'''CREATE TABLE IF NOT EXISTS
			markov_model
			(
			word_1 TEXT NOT NULL,
			word_2 TEXT NOT NULL,
			word_3 TEXT NOT NULL,
			word_4 TEXT NOT NULL,
			word_5 TEXT NOT NULL,
			count INTEGER NOT NULL DEFAULT 0,
			PRIMARY KEY (word_1, word_2, word_3, word_4, word_5)
			)
			''')

	def add_many(self, fivegrams, count = None):
		fivegrams = [(fivegram[0], fivegram[1] or '', fivegram[2] or '', fivegram[3] or '', fivegram[4] or '', fivegram[5] or '')
					for fivegram in fivegrams]

		with self.connection:
			self.connection.executemany(
				'''INSERT OR IGNORE INTO markov_model
				(word_1, word_2, word_3, word_4, word_5) VALUES (?, ?, ?, ?, ?)
				''',
				list(map(lambda x: x[1:], fivegrams))
			)
			self.connection.executemany(
				'''UPDATE markov_model
				SET count = ?
				WHERE word_1 = ? AND word_2 = ? AND word_3 = ? AND word_4 = ? AND word_5 = ?
				''',
				fivegrams
			)

	def get_ngram_values(self, word_1, word_2, word_3, word_4):
		query = self.connection.execute(
			'''SELECT word_5, count FROM markov_model
			WHERE word_1 = ? AND word_2 = ? AND word_3 = ?  AND word_4 = ?
			ORDER BY count DESC LIMIT 1000
			''',
			(word_1 or '', word_2 or '', word_3 or '', word_4 or '')
		)

		value_dict = collections.OrderedDict()

		for row in query:
			value_dict[row[0] or None] = row[1]

		if not value_dict:
			raise KeyError()

		return value_dict
	