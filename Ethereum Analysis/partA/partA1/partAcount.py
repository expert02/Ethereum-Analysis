#importing mrjob to run mapreduce
from mrjob.job import MRJob
import time
from mrjob.step import MRStep

class PartA1(MRJob):
    def mapper(self, _, line):
        try:
            #spliting the list
            fields = line.split(',')
            if len(fields) == 7: #transactions
                 #declering row timestamp and giving the limit 6 of the list
                Rt = int(fields[6])
                #setting the time
                y_m = time.strftime('%Y-%m', time.gmtime(Rt))
                yield (y_m, 1)
        except:
            pass

    def combiner(self, key, value):
            yield (key, sum(value))
#inserting the action sum in reducer
    def reducer(self, key, value):
            yield (key, sum(value))

    #def steps(self):
        #return [MRStep(mapper=self.mapper,combiner = self.combiner, reducer=self.reducer, jobconf={'mapreduce.job.reduces': '10'})]

#number of reducers 10
if __name__ == '__main__':
    PartA1.JOBCONF = {'mapreduce.job.reduces': '10'}
    PartA1.run()
#Submitted application application_1648683650522_4263
