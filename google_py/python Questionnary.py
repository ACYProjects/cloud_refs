questions = [
    {
        "question": "What is the capital of France?",
        "choices": ["Paris", "London", "Berlin", "Madrid"],
        "correct_choice": "Paris"
    },
    {
        "question": "Which of these programming languages is object-oriented?",
        "choices": ["Python", "C", "Java", "Assembly"],
        "correct_choice": "Java"
    },
    {
        "question": "What is the symbol for the element sulfur?",
        "choices": ["S", "Cu", "Ag", "Fe"],
        "correct_choice": "S"
    }
]

score = 0

for question in questions:
    print(question["question"])
    for i, choice in enumerate(question["choices"]):
        print(f"{i + 1}. {choice}")
    user_choice = input("Enter the number of your choice: ")
    if question["choices"][int(user_choice) - 1] == question["correct_choice"]:
        score += 1
        print("Correct!")
    else:
        print("Incorrect.")

print(f"You scored {score} out of {len(questions)}.")

