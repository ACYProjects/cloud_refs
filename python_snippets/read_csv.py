import csv

path_file = 'file_path'

with open(path_file, 'r') as file:
    reader = csv.reader(file)
    for row in reader:
        print(row)
