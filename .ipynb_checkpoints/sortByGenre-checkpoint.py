# -*- coding: utf-8 -*-
"""
Created on Sun May  1 15:07:17 2022

@author: munat
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

genres = {
          4:"Unknown", 5:"Action", 6:"Adventure", 8:"Animation", 9:"Children's", 10:"Comedy",11:"Crime",
          12:"Documentary",13:"Drama",14:"Fantasy",15:"Film-Noir",16:"Horror", 17:"Musical",18:"Mystery",19:"Romance",20:"Sci-Fi",
          21:"Thriller", 22:"War",23:"Western"
}

class SortByGenre(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.sum_rating),
            MRStep(mapper = self.sort_by_genre)
            ]
    
    def mapper(self,_,row):
        details = row.split('\t')
        for i in range(4, len(genres)):
            if(details[i] == 1):
                yield genres[i], int(details[3])

    def sum_rating(self, genre, rating):
        yield None, (sum(rating), genre)
        
    def sort_by_genre(self, _,sorted_list):
        for pair in sorted_list:
          yield pair[1], pair[0]
          
if __name__ == '__main__':
    SortByGenre.run()