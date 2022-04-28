import pyspark
from operator import add

sc = pyspark.SparkContext()

def is_good_line(line):
    try:
        fields = line.split(',')
        if len(fields) == 7: # transactions
            str(fields[2]) # to_addr
            if int(fields[3]) == 0:
                return False
        elif len(fields) == 5: # contracts
            str(fields[0]) # contract addr
        else:
            return False
        return True
    except:
        return False

transactions = sc.textFile('/data/ethereum/transactions').filter(is_good_line)
contracts = sc.textFile('/data/ethereum/contracts').filter(is_good_line)


step1 = transactions.map(lambda l: (l.split(',')[2], int(l.split(',')[3])))
step2 = step1.reduceByKey(add)
step3 = step2.join(contracts.map(lambda x: (x.split(',')[0], 'contract')))
top10 = step3.takeOrdered(10, key = lambda x: -x[1][0])

for record in top10:
    print("{},{}".format(record[0], int(record[1][0]/1000000000000000000)))
#application_1649894236110_1689 15 minutes try 1
#application_1649894236110_1779 10 minutes
#application_1649894236110_1753 10 minutes

