from mrjob.job import MRJob
import time
from mrjob.step import MRStep
class PartA2(MRJob):

    def mapper(self, _, line):
        try:
            fields = line.split(',')
            Rt = int(fields[6])
            #converting into eth value and float fields 3 stands for value [0,1,2,3] array
            trn_v = float(fields[3]) / 1000000000000000000
            y_m = time.strftime('%Y-%m', time.gmtime(Rt))
            yield (y_m, {'count': 1, 'trn_v': trn_v})
        except:
            pass

    def combiner(self, key, value):
        totalvalue = 0.0
        totalcount = 0

        for val in value:
            totalcount += val['count']
            totalvalue += val['trn_v']

        result = { 'count': totalcount, 'trn_v': totalvalue }

        yield (key, result)

    def reducer(self, key, value):
        totalvalue = 0.0
        totalcount = 0

        for val in value:
            totalcount += val['count']
            totalvalue += val['trn_v']

        avgvalue = totalvalue / totalcount

        yield (key, avgvalue)
    def steps(self):
        return [MRStep(mapper=self.mapper,combiner = self.combiner, reducer=self.reducer, jobconf={'mapreduce.job.reduces': '10'})]


if __name__ == '__main__':
    PartA2.run()
#job_1648683650522_4404
