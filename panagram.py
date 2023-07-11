def panagram(s):
         if len(set('abcdefghijklmnopqrstuvwxyz')-set(s.lower()))==0:
           return True
         return False

user_string=input("enter a word :")

if panagram(user_string):
         print("panagram")
else:
         print("no panagram")