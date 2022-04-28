#Job 1 - Initial Aggregation

#To workout which services are the most popular, you will first have to aggregate transactions to see
#how much each address within the user space has been involved in. You will want to aggregate value for addresses in the to_address field.
#This, in essense, will be similar to the wordcount problem that we saw in Lab 1 and Lab 2.
#Job 2 - Joining transactions/contracts and filtering
#Once you have obtained this aggregate of the transactions, the next step is to perform a repartition join between this aggregate and contracts
# You will want to join the to_address field from the output of Job 1 with the address field of contracts .
#Secondly, in the reducer, if the address for a given aggregate from Job 1 was not present within contracts this should be filtered out as it is a user address and not a smart contract.
#Job 3 - Top Ten
#Finally, the third job will take as input the now filtered address aggregates and sort these via a top ten reducer, utilizing what you have learned from lab 4.
from mrjob.job import MRJob
from mrjob.step import MRStep

class T10S(MRJob):
    def mapper_filter(self, _, line):
        try:
            if len(line.split(",")) == 7: #transactions
                fields = line.split(",")
                to_address = fields[2]
                value = int(fields[3])

                yield to_address, ("value", value)

            elif len(line.split(",")) == 5: #contracts
                fields = line.split(",")
                address = fields[0]

                yield address, "contract"
        except:
            pass

    def reducer_filter(self, key, values):
        bool = False
        val = []

        for value in values:
            if value[0] == "value":
                val.append(value[1])
            elif value == "contract":
                bool = True

        if bool:
            yield key, sum(val)

    def mapper_sort(self, key, value):
        yield None, (key, value)

    def reducer_sort(self, _, key_pair):
        sorted_values = sorted(key_pair, reverse=True, key=lambda x: x[1])

        for value in sorted_values[:10]:
            yield value[0], value[1]

    def steps(self):
        return [MRStep(mapper=self.mapper_filter,
                       reducer=self.reducer_filter),
                MRStep(mapper=self.mapper_sort,
                       reducer=self.reducer_sort)]

if __name__ == '__main__':
    T10S.run()
    #job_1649894236110_1390 42 minutes
    #job_1649894236110_1269 40 minutes
    #application_1649894236110_1630 40 minutes
