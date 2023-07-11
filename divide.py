def outer(ref):
    def wrapper(a,b):
        if b==0:
            print('b is zero')
        else:
            ref(a,b)

    return wrapper
@outer
def div(a,b):
    print(int(a/b))


div(4,0)