from csv import reader
import itertools

# Remove the characters that python doesn't like
# from the dataset file, also update the way data
# is pulled this isn't needed

# TODO: everything
def main():
    i = 0
    with open("data/dataset.csv") as f:
        for c in itertools.chain.from_iterable(f):
            if (ord(c) > 128):
                print("fuck at {} with {}".format((i),ord(c)))
                break
            i += 1

if __name__ == "__main__":
    main()