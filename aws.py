import json

d={"a":"ronit"}

c=json.dumps(d)

with open(r"C:\Users\ronkumar\Desktop\sample.txt","r") as r:
    s=r.read()
books=json.loads(s)
print(type(books))
print(books)