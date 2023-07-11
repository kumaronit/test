def reverseString(string):
  #string="the file is a open"
    str=string.split(" ")
    str2=""
    for i in str[::-1]:
           str2+=i+" "

    return str2.strip()

print(reverseString("they help us"))