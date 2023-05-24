with open('filename.txt', 'r') as file:
    lines = file.readlines()
    if len(lines) >= 2:
        second_line = lines[1]
        print(second_line)
