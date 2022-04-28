#importing libraries
from mrjob.job import MRJob
import time

class Complexity(MRJob):
    def mapper(self, _, lines):
        try:
            fields = lines.split(",")
            if len(fields) == 9: #blocks dataset
                timestamp = int(fields[7]) #[from the dataset 7 stands for timestamp [count starts from 0]]
                dif = int(fields[3])
                year = time.strftime("%Y", time.gmtime(timestamp))
                month = time.strftime("%m", time.gmtime(timestamp))
                key = str(year) + '-' + str(month)

                yield key, dif
        except:
            pass

    def reducer(self, key, values):
        price = 0
        count = 0

        for v in values:
            price += v
            count += 1

        yield key, price/count

if __name__ == '__main__':
    Complexity.run()
    # http://andromeda.student.eecs.qmul.ac.uk:8088/proxy/application_1649894236110_1883/
