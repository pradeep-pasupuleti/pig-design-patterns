#! /usr/bin/env python

# Import required modules 
import sys
import csv
from nltk.metrics.distance import masi_distance

# Set the distance function to use and the distance threshold value 
DISTANCE_THRESHOLD = 0.5
DISTANCE = masi_distance

def cluster_contacts_by_title():

# Read data from stdin and store in a list called contacts
    contacts = [line.strip() for line in sys.stdin]
    for c in contacts[:]:
        if len(c)==0 :
           contacts.remove(c)


# create list of titles to be clustered (from contacts list) 
    all_titles = []
    for i in range(len(contacts)):
        title = [contacts[i]]
        all_titles.extend(title)

    all_titles = list(set(all_titles))


    # calculate masi_distance between two titles and cluster them based on the distance threshold, store them in dictionary variable called clusters
    clusters = {}
    for title1 in all_titles:
        clusters[title1] = []
        for title2 in all_titles:
            if title2 in clusters[title1] or clusters.has_key(title2) and title1 \
                in clusters[title2]:
                continue
            distance = DISTANCE(set(title1.split()), set(title2.split()))
            if distance < DISTANCE_THRESHOLD:
                clusters[title1].append(title2)


    # Flatten out clusters
    clusters = [clusters[title] for title in clusters if len(clusters[title]) > 1]

    # Write the cluster names to stdout
    for i in range(len(clusters)):
    	print ", ".join(clusters[i])


# Main Function
if __name__ == '__main__':
    cluster_contacts_by_title()

