people = [
    {'name': 'Alice', 'age': 25},
    {'name': 'Bob', 'age': 30},
    {'name': 'Charlie', 'age': 20}
]

people_sorted = sorted(people, key=lambda x: x['age'])
print(people_sorted)
