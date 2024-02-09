import cv2
import pytesseract

filepath = 'C:/Users/hardo/Documents/Misc/Exams/'
file = '14h_1.png'

# Load the image
image = cv2.imread(filepath + file)

# Convert the image to grayscale
gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)

# Apply thresholding to the image to convert it to black and white
thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY_INV | cv2.THRESH_OTSU)[1]

# Perform OCR on the image
text = pytesseract.image_to_string(thresh)

# Manually parse the text to extract the table data
rows = text.split('\n')
table = [row.split('\t') for row in rows]

# Print the extracted table
for row in table:
    print(row)
