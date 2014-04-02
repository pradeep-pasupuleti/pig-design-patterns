#! /usr/bin/env python

# Import required modules 

import sys
import string
import nltk


# Read data from stdin and store it as sentences
for line in sys.stdin:
    if len(line) == 0: continue
    sentences = nltk.tokenize.sent_tokenize(line)

    # Extract words from sentences
    words = [nltk.tokenize.word_tokenize(s) for s in sentences]

    # Extract Part of Speech from words
    pos_words = [nltk.pos_tag(t) for t in words]

    # Chunk the extracted Part of Speech tags
    named_entities = nltk.batch_ne_chunk(pos_words)

    # Write the chunks to stdout
    print named_entities[0]
