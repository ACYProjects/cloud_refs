my_string = "hello world capitalize all words"
words = my_string.split() 
capitalized_words = []

for word in words:
    capitalized_words.append(word.capitalize())  

capitalized_string = " ".join(capitalized_words) 
print(capitalized_string)

