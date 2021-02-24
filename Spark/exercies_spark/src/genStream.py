import time
with open("fileToStream.txt", "r") as fp: 
    Lines = fp.readlines()
    for line in Lines:
        print(line)
        