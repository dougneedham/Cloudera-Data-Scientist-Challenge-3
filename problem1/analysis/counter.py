#!/usr/bin/python

import sys

current_word = None
current_count = 0
word = None

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    origin,airport, count = line.split(',')
    word = origin+airport

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    if current_word == word:
        current_count += count
    else:
        if current_word:
	    if current_count > 1:
                print '%s,%s,%s' % (origin,airport, current_count)

        current_count = count
        current_word = word

# do not forget to output the last word if needed!
if current_word == word:
    print '%s,%s,%s' % (origin,airport, current_count)

