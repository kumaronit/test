class Dog:
    species="new"
    def __init__(self,name,age):
        self.name=name
        self.age=age

a=Dog("sheru",10)
a.age=15
a.species="old"
print(Dog.species)

print(a.__dict__)
