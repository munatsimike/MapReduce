# -*- coding: utf-8 -*-
"""
Created on Sun May  1 15:07:17 2022

@author: munat
"""

from mrjob.job import MRJob
from mrjob.step import MRStep

genres = {
          4:"Unknown", 5:"Action", 6:"Adventure", 7:"Animation", 8:"Children's", 9:"Comedy",10:"Crime",
          11:"Documentary",12:"Drama",13:"Fantasy",14:"Film-Noir",15:"Horror", 16:"Musical",17:"Mystery",18:"Romance",19:"Sci-Fi",
          20:"Thriller", 21:"War",22:"Western"
}

class SortByGenre(MRJob):
    
    def steps(self):
        return [
            MRStep(mapper = self.mapper,
                   reducer = self.sum_rating),
            MRStep(mapper = self.sort_by_genre)
            ]
    
    def mapper(self,_,row):
        data = row.split('\t')
        for i in range(4, len(genres)+ 4):
            if(data[i] == '1'):
                yield genres[i], int(data[3])

    def sum_rating(self, genre, rating):
        yield None, (sum(rating), genre)
        
    def sort_by_genre(self,_,values):
        yield values[1], values[0]
          
if __name__ == '__main__':
    SortByGenre.run()