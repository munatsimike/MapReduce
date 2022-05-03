# -*- coding: utf-8 -*-
"""
Created on Sun May  1 13:19:40 2022

@author: munat
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

class SortByRating(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.map_rating,
                       reducer = self.count_rating),
            MRStep(reducer = self.sort_by_rating)
            ]
    
    def map_rating(self,_, row):
        details = row.split('\t')
        yield details[1], details[2]
        
    def count_rating (self, movie_id, rating):
        yield None, (len(list(rating)), movie_id)
        
        
    def sort_by_rating(self,_, value):
        sorted_pairs = sorted(value)
        for pair in sorted_pairs:
            yield pair[1], pair[0]

if __name__ == '__main__':
    SortByRating.run()