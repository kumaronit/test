def frequency(list):
    d={}
    for i in list:
        if i in d.keys():
            d[i]+=1
        else:
            d[i]=1


print(frequency([1,2,3,1]))