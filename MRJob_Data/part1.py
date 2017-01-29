#!/usr/bin/python

import mrjob
from mrjob.job import MRJob
import re

WORD_RE = re.compile(r"[^\w']?([\w']+)[^\w']?")
# what is the different between ? and no ?

class MRMostUsedWord(MRJob):

  def mapper(self, _, line):
    words = WORD_RE.findall(line)
    for i in range(len(words)):
        yield (words[i]).lower(),1

  def combiner(self, word, counts):
    yield word, sum(counts)

  def reducer(self, word, counts):
    yield word, sum(counts)

if __name__ == '__main__':
  MRMostUsedWord.run()
