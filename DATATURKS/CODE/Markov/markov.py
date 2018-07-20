from pprint import pprint
import random
import sys
import json
import argparse
# import nltk
from store import *
from file_process import FileHandle

class MarkovChain:

	def __init__(self, db = ':memory:', file = '', n = 3):
		self.memory = {}
		self.n = n
		self.file = file
		self.db = db
		self.store = self.__choose_store()
		self.ngrams = []
	
	def __choose_store(self):
		db_name = self.db
		if(self.n == 2):
			return Bigram(db_name)
		elif(self.n == 3):
			return Trigram(db_name)
		elif(self.n == 4):
			return Fourgram(db_name)
		elif(self.n == 5):
			return Fivegram(db_name)
		else:
			raise NotImplementedError("%d-gram not implemented"%self.n)
	
	def process_file(self, ret=False):
		with open(self.file, 'r', encoding = "ISO-8859-1") as f:
			for line in f:
				self.ngrams.append(line.split())
				
		if(ret):
			return
		# print(self.ngrams)
		self.to_store(self.ngrams)
	
	def __learn_key(self, *key, value):
		if key not in self.memory:
			self.memory[key] = []
		self.memory[key].append(value)
	
	def to_store(self, ngrams):
		self.store.add_many(ngrams)
	
	def query(self, *words):
		return (self.store.get_ngram_values(*words))
	
	def learn(self, tokens):
		ngrams = [[tokens[i+j] for j in range(self.n)] for i in range(0, len(tokens) - (self.n-1))]
		
		# for ngram in ngrams:
			# self.__learn_key(*ngram[:self.n-1], value = ngram[-1])
		return ngrams[0]
		
	def next(self, *current_state):
		next_possible = self.memory.get(current_state)
		if not next_possible:
			next_possible = self.memory.keys()

		return random.sample(next_possible, 1)
	
	def learn_from_text(self):
		print('Learning %s...'%self.file)
		total_n_grams = 0
		p = FileHandle(options.data_file, options.n)
		for i, ngrams in enumerate(p.get_n_grams()):
			print('Line: %d'%i)
			for ngram in ngrams:
				total_n_grams+=1
				self.ngrams.append(ngram)
		print('Total: %d'%total_n_grams)
		self.to_store(self.ngrams)
		
	def validate(self):
		p = FileHandle(options.data_file, options.n)
		pos = 0
		neg = 0
		for i, ngrams in enumerate(p.get_n_grams()):
			print('Line: %d POS: %d NEG: %d'%(i, pos, neg))
			for ngram in (ngrams):
				try:
					res = self.query(*ngram[:-1])
					# if(ngram[-1] in res):
					pos+=1
				except:
					neg+=1
		
		return (pos, neg)
		
	def validate_coca(self, random_test = False):
		pos = 0
		neg = 0
		self.process_file(True)
		
		if(random_test):
			count = 500
			while(count > 0):
				i = random.randint(1, 1000000)
				ngram = self.ngrams[i]
				print('Count: %d POS: %d NEG: %d'%(count, pos, neg))
				try:
					res = self.query(*ngram[:-1])
					# if(ngram[-1] in res):
					pos+=1
				except:
					neg+=1
				count-=1
		return(pos, neg)
		
		for i, ngram in enumerate(self.ngrams):
			print('Line: %d POS: %d NEG: %d'%(i, pos, neg))
			try:
				res = self.query(*ngram[:-1])
				# if(ngram[-1] in res):
				pos+=1
			except:
				neg+=1

if __name__ == '__main__':
	parser = argparse.ArgumentParser(description = 'usage %prog ' + '-m<model>/-d<dataset> -n<n-gram> ')
	parser.add_argument('-w', dest='dataset', type = str, action = 'store', help='Weights to train')
	parser.add_argument('-f', dest='data_file', type = str, action = 'store', help='Text file to train/test')
	parser.add_argument('--test', action='store_true')
	parser.add_argument('--train', action='store_true')
	parser.add_argument('--coca', action='store_true')
	parser.add_argument('-m', dest='model',  type = str, action = 'store', help='Trained model')
	parser.add_argument('-n', dest='n',  type = int, action = 'store', help='N in N-gram')
	parser.add_argument('-words', dest='words',  type = int, action = 'store', help='Next n words to generate')
	parser.add_argument('--predict', nargs = "*", dest = 'predict', action='append')
	
	options = parser.parse_args()
	
	# if(options.test):
		# # python3 markov.py -m database/HP1.db -f dataset/HP7.txt --test -n 3
		# m = MarkovChain(options.model, options.data_file, options.n)
		# print(m.validate())
	
	# if(options.train):
		# m = MarkovChain(options.dataset, options.data_file, options.n)
		# m.learn_from_text()
		
	# elif(options.coca):
		# m = MarkovChain(options.dataset, options.data_file, options.n)
		# m.process_file()
		
	# elif(options.model != None):
		# print('call')
		# m = MarkovChain(options.model, n = options.n)
	# else:
		# # print('call')
		# print(parser.usage)
		# exit(0)
	if(options.predict != [] and options.test):
		# python3 markov.py -w database/w3_small.db -n 3 --test --coca --predict a baby
		m = MarkovChain(options.dataset, n =options.n)
		pprint(m.query(*(options.predict[0])))
	if(options.predict != [] and options.words != None):
		#python3 markov.py -w database/big/w3.db -n 3 -words 100 --predict are you
		m = MarkovChain(options.dataset, n = options.n)
		pprint(m.query(*(options.predict[0])))
		ngram = options.predict[0]
		for i in range(options.words):
			# print(ngram)
			try:
				res = m.query(*ngram).keys()
				word = list(res)[random.randint(0, 2)]
				print(word, end = ' ')
			except:
				pass
			ngram.pop(0); ngram.append(word)
			
		print()
	
# Result: [TODO]: chi-square, tabulate, error	
# Line: 1020385 POS: 943345 NEG: 77040 wiki - w2_.txt 92.44
# Line: 1020008 POS: 977609 NEG: 42399 wiki - w3_.txt 95.84
# Line: 1034307 POS: 967401 NEG: 66906 wiki - w4_.txt 93.53
# Line: 1044268 POS: 971609 NEG: 72659 wiki - w5_.txt 93.042
	
