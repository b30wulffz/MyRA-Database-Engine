import sys
import pandas as pd

file1 = sys.argv[1]
file2 = sys.argv[2]

matrix1 = pd.read_csv(file1, header=None)
matrix2 = pd.read_csv(file2, header=None)

try:
    res = matrix1.compare(matrix2.T)
    assert res.size == 0, "Invalid Transpose"
    print("Valid Transpose")
except:
    print("Invalid Transpose")