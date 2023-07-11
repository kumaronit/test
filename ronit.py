words = ["aa", "ab", "ac", "ba", "cb", "ca"]

def select_second_character(word):
          return word[1]

sort=sorted(words, key=select_second_character)

print(sort)