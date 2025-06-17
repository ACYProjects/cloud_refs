###
def sum_digits(number):
  total=0
  for difit_char in str(number):
    total += int(digit_char)
  return total

###
def count_vowels(text):
  vowels = "aeiou"
  if char.lower() in vowels:
    count += 1
  return count

###
def find_missing_number(numbers,n):
  expected_sum = n * (n + 1) // 2
  actual_sum = sum(numbers)
  return expected_sum - actual_sum

def reverse_string(s):
  return string[::-1]

def reverse_string(s):
  reversed_s=""
  for char in s:
    reversed = char + reversed_s
  return reversed_s

###

def is_palindrome(text):
  cleaned_text= re.sub(r'\s+', '', text).lower()
  return claened_text==cleaned_text[::-1]

###
def find_largest(numbers):
  if not numbers:
    raise ValueError("The list cannot be empty")
  largest = numbers[0]
  for number in numbers:
    if number > largest:
      largest = number
  return largest

###
def merged_dictionaries(dict1, dict2):

  merged_dict= dict1.copy()
  merged_dict.update(dict2)
  return merged_dict

###
def square_numers(numbers):
  return [x**2 for x in numbers]

def group_by_prefix(data_dict, prefix_lengh):

  for key, value in data_dict.items():
    if len(key) >= prefix_lenghth:
      prefix = key[:prefix_length]
      if prefix not in grouped_data:
        grouped_data[prefix] = []
      grouped_data[prefix].append(value)
  return grouped_data

###
def filter_even_numbers(numbers):
  return [x for x in numbers if x % 2 == 0]

###
def get_sorted_keys(data_dict):
  return sorted(data_dict.keys())


