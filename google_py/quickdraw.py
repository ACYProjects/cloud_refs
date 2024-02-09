from quickdraw import QuickDrawData
import os

os.chdir('C:/Users/hardo/Documents/Google Data Enginner/Smart Analytics Machine Learning  and AI on GCP')

qd = QuickDrawData()

for i in range(0,3):  
    
    anvil = qd.get_drawing("anvil")
    anvil.image.save("my_anvil" + str(i) + ".gif")

