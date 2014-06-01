import sys
import os
import csv
from itertools import combinations, chain, islice
from collections import Counter
import time

import json
from pprint import pprint
     
class OrderedTuple(tuple):

    def __init__(self, init=()):
        if init:
            init = list(init)
            init.sort()
            init = tuple(init)
            self._oinit = init
            tuple.__init__(init)
        else:
            tuple.__init__(self)

    def __eq__(self, other):
        return self._oinit[0] == other._oinit[0] and self._oinit[1] == other._oinit[1]
        # return self._oinit == other._oinit

    def __hash__(self):
        return hash(self._oinit)

    def __repr__(self):
        return repr(self._oinit)


if __name__ == '__main__':

    with open('page_fas.csv', 'rb') as fo:
        content = [ row for row in csv.reader(fo, delimiter=';') ]

    _t = time.time()
    wordlists={}
    # { 'user': { 'words': [ set of non empty words ]}, ... }
    i=0
    for row in content:
        i+=1
        user = row[0]
        if (i%100==0):
            sys.stdout.write('\r%s' % user)
            sys.stdout.flush()
        wordlists[user] = {}
        wordlists[user]['words']=set([s for s in row[2:] if s != ''])
        _tuples = combinations(wordlists[user]['words'],2)
        tuples = set()
        for t in _tuples:
            tuples.add(OrderedTuple(t))
        wordlists[user]['tuples'] = tuples # unique ordered tuples
        wordlists[user]['hashes'] = set([ hash(t) for t in tuples ])
        assert len(tuples) == len(wordlists[user]['hashes'])
        # print('%s, %s' % (user, len(tuples)))
    delta = time.time()-_t
    print('\n%d data collected in %dsec.' % (i, delta))

    iterators = []
    for user in wordlists.iterkeys():
        iterators.append(iter(wordlists[user]['tuples']))

    hash_dict = {}
    # { hash_value: OrderTuple, ... }
    print('Making hash_dict ...')
    _t=time.time()
    i=0
    for _tuple in chain(*iterators):
        i+=1
        hash_dict[hash(_tuple)] = _tuple
        if (i%100000 == 0):
            sys.stdout.write('\r%d' % i)
            # sys.stdout.write('\r%d (%r) %r' % (i, type(_tuple), repr(_tuple)))
            sys.stdout.flush()
    print('\n%d hash_dict done in %dsec.' % (i, delta))
    

    iterators = []
    for user in wordlists.iterkeys():
        iterators.append(iter(wordlists[user]['hashes']))
    _stats = {}
    done = []
    i=0

    print('Counting ...')
    _t = time.time()
    for t in chain(*iterators):
        if t in done:
            continue
        i+=1
        if (i%1000 == 0):
            sys.stdout.write("\r%s" % i)
            sys.stdout.flush()
        done.append(t)
        _count = 0
        for user in wordlists.iterkeys():
            if t in wordlists[user]['hashes']:
                _count += 1
        _stats[hash(t)]=_count

    delta = time.time() - _t
    print('\nDone writing in %dsec.' % (i, delta))

    print('Preparing data to write ... ')
    stats = []
    error_hashes = []
    for _hash, _tuple in hash_dict.iteritems():
        try:
            _count = _stats[_hash]
        except KeyError as e:
            error_hashes.append(e)
        else:
            stats.append((_tuple, _count))
    print('error_hashes=%r' % len(error_hashes))

    print('Writting data')
    with open('stats.json', 'w') as fo:
        fo.write(json.dumps(stats))
    print('DONE.')
