import csv
from distutils.log import error
import itertools

# Remove the characters that python doesn't like
# from the dataset file, also update the way data
# is pulled this isn't needed

# TODO: everything
def main():
#     with open("data/fixedData.csv", "w", encoding="ascii", errors="ignore") as wFile:
#         with open("data/dataset.csv", "r", encoding="ascii", errors="ignore") as rFile:  
#             reader = csv.reader(rFile)
#             writer = csv.writer(wFile)
#             for rows in reader:
#                 writer.writerow(rows)
    i = 0
    with open("data/fixedData.csv", "r") as rFile:
        for c in itertools.chain.from_iterable(rFile):
            # print(ord(c))
            if ord(c) > 128:
                break
            # if (ord(c) > 128):
            #     if broken == False:
            #         broken = True
            #         continue
            #     print("fuck at {} with {}".format((i),ord(c)))
            #     break
            i += 1

if __name__ == "__main__":
    main()