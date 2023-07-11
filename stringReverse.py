def positiveNumberCount(arr):
    count=0
    for i in arr:
        if i>0:
            print(i)
            count +=1
    return count
print(positiveNumberCount([1,-2,3]))