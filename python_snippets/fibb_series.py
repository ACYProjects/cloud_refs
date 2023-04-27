nums = 7
lst = []
counter = 0
num1 = 0
num2 = 1

if nums > 0 :
    while counter < nums:
    
        res = num1 + num2
        lst.append(res)
    
        num1 = num2
        num2 = res
    
        counter += 1
    
    print(lst)
