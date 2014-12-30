
import sys
from pprint import pprint

from edx.idea.data_frame import DataFrame


def main():
    df = DataFrame.from_url(sys.argv[1])
    pprint(df.take(5))
    pprint(df.count())


if __name__ == '__main__':
    main()
