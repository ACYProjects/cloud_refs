# Define the quiz questions and answers
questions = ['What is the capital of France?', 'What is the currency of Japan?']
answers = ['Paris', 'Yen']
choices = [['Paris', 'London', 'Rome'], ['Dollar', 'Euro', 'Yen']]

# Keep track of the number of correct answers
correct = 0

# Loop through the questions
for i in range(len(questions)):
  print(questions[i])
  for j in range(len(choices[i])):
    print(str(j+1) + ': ' + choices[i][j])
  
  # Get the user's response
  response = input('Enter your choice: ')
  
  # Check if the response is correct
  if response.isdigit():
    response = int(response)
    if response > 0 and response <= len(choices[i]):
      if choices[i][response-1] == answers[i]:
        print('Correct!')
        correct += 1
      else:
        print('Incorrect. The correct answer is: ' + answers[i])
    else:
      print('Invalid choice')
  else:
    print('Invalid input')

# Display the final score
print('You got ' + str(correct) + ' out of ' + str(len(questions)) + ' questions correct')


