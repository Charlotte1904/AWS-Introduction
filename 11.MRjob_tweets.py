
from mrjob.job import MRJob
from mrjob.step import MRStep
import json



class MRJobtophashtags(MRJob):
	def steps(self):
		# run first 3 functions in serial - then run finalizer reducer from all pre_reducer outputs
		return [
			MRStep(mapper=self.mapper,
				   combiner=self.combiner,
				   reducer=self.reducer),
			MRStep(reducer=self.reducer_top_10)
		]

	def mapper(self,_,line,):
		# each line in tweets ( already seperated by \n)
		try:
			if json.loads(line)["entities"]["hashtags"] != []:
				for hashtags in json.loads(line)["entities"]["hashtags"]:
					hashtag = hashtags["text"]
					yield (hashtag, 1)	
		except ValueError:
			pass

	def combiner(self, hashtag, counts):
		yield (hashtag, sum(counts))
	
	def reducer(self, hashtag, counts):
		# all the variables to have the same key - return one file 
		yield None, (sum(counts), hashtag)

	def reducer_top_10(self, _ , word_count_pairs):
		# discard the key; it is just None
		# sort the list of tuples basing on the first value ( which is the counts) in descending
 		top_10 = sorted(word_count_pairs, key = lambda x: x[0], reverse = True)
		for x in top_10[:10]:
		  yield x 
		


if __name__ == '__main__':
	MRJobtophashtags.run()



# to run this python MRjob_tweets.py file/ 2> counts2
