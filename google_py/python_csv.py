import csv

path_file = 'C:/Users/hardo/Anaconda3/Lib/site-packages/numpy/random/tests/data/pcg64-testset-2.csv'

with open(path_file, 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)

