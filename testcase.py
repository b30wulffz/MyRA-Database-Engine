import sys
import random
import numpy

rows = int(sys.argv[1])
cols = int(sys.argv[2])
sparse = int(sys.argv[3])
name = sys.argv[4]
size = rows*cols
cutoff = int(size*0.4)
count = 0

matrix = []

with open('./data/TC_{}.csv'.format(name), 'w') as f:
    for x in range(rows):
        row = []
        for y in range(cols):
            value = random.randint(0,10000)
            if sparse:
                if(random.randint(0,size) > int(size/4)):
                    value = 0
                if value != 0:
                    if count >= cutoff :
                        value = 0
                    else:
                        count+=1
            row.append(value)
        matrix.append(row)
        print(', '.join(str(i) for i in row), file = f)

matrix = numpy.transpose(matrix)

with open('./data/TC_{}_TS.csv'.format(name), 'w') as f:
    for row in matrix:
        print(', '.join(str(i) for i in row), file = f)
