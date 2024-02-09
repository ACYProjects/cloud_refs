import pytesseract
from PIL import Image

filepath = 'C:/Users/hardo/Documents/Misc/Exams/'
pytesseract.pytesseract.tesseract_cmd = 'C:/Program Files/Tesseract-OCR/tesseract.exe'

image = Image.open(filepath + '10h.png')
image = image.convert('1')

text = pytesseract.image_to_string(image)

print(text)

