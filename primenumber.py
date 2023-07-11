def factor(n):
    list=[]
    for i in range(1,n+1):
        if n%i==0:
             list.append(i)
    return(list)


def isPrime(n):
    if factor(n)==[1,n]:
        print("{} is a prime".format(n))
    else:
        print("{} is a not prime".format(n))


isPrime(6)
