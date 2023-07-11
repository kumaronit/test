def outter(ref):
    def wrapper(lst):
        if 0 in lst:
            print("0 is present in list")
        else:
            ref(lst)
    return wrapper

@outter
def product(lst):
    result=1
    for i in lst:
        result *=i
    print(result)


#getProduct=outter(product)
#getProduct([1,0,4])


product([1,0,3])