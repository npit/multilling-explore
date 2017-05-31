
import  os, pickle
from itertools import combinations
import mysql.connector

#################################
# user settings here
# langs_of_interest=['en','el','it','zh']
langs_of_interest = ['zh', "el"]
credsfile = "mysql.conf"

#################################

creds = []
with open(credsfile,'r') as f:
    for line in f:
        creds.append(line.strip())

user  = creds[0]
passw = creds[1]
database = creds[2]
noLoad = True
if not os.path.exists('db') or noLoad :
    # start the connection #############################
    try:
        cnx = mysql.connector.connect(user=user, password=passw,  database=database, charset="utf8")
        cursor = cnx.cursor()
    except:
        print("Unable to connect to the database.")
        exit(1)

    ####################################################



    query = ("SELECT annotator_uid, p_first_summary_id, p_second_summary_id from annotation")
    cursor.execute(query)
    annotations = cursor.fetchall()

    query = ("SELECT id, topic_id from p_summary")
    cursor.execute(query)
    summ = cursor.fetchall()

    query = ("SELECT id, topic_name, lang_code from topic")
    cursor.execute(query)
    topics = cursor.fetchall()

    topics_per_lang = {}
    sumpairs_per_topic = {}

    print("Parsing stuff...")
    for trow in topics:

        lang = trow[2]
        topic = trow[0]

        # limit to lang
        if langs_of_interest:
            if not lang in langs_of_interest:
                continue

        if not lang in topics_per_lang:
            topics_per_lang[lang] = []

        topics_per_lang[lang].append(topic)
        if not topic in sumpairs_per_topic:
            sumpairs_per_topic[topic] = [[],[]] # summary pairs, annotations for said pairs

        summaries = []
        # get all summaries for topic
        for s in summ:
            if s[1] == topic:
                summaries.append(s[0])
        # make combos of summaries
        sumpairs = list(combinations(summaries,2))
        # sort each combo
        for i in range(len(sumpairs)):
            l = list(sumpairs[i])
            l.sort()
            sumpairs[i] = tuple(l)
        # add to topic
        sumpairs_per_topic[topic][0] = sumpairs

        # add annotation slots
        sumpairs_per_topic[topic][1] = [ [] for j in range(len(sumpairs)) ]
        # get annotations per pair of topic
        for ann in annotations:
            pair = [ ann[1], ann[2]]
            pair.sort()
            pair = tuple(pair)
            if pair in sumpairs:
                idx = sumpairs.index(pair)
                sumpairs_per_topic[topic][1][idx].append(ann[0])

        with open('db','wb') as f :
            pickle.dump([topics_per_lang, sumpairs_per_topic], f)
else:
    f = open('db','rb')
    topics_per_lang, sumpairs_per_topic = pickle.load(f)
    f.close()

# print info
max_annot = 3
count_pair = 0

for lang in topics_per_lang:
    annotPerLangCounter = 0
    count_topic = 0
    print("LANG %s :" % lang)
    empty_topics = []
    complete_pairs = []
    for topic in topics_per_lang[lang]:
        count_topic = count_topic  + 1
        pairs_annotations = sumpairs_per_topic[topic]
        pairs = pairs_annotations[0]
        annotations = pairs_annotations[1]
        annotationsPerPairForTopic = list(map(len,annotations))
        annotationsForTopic = sum(annotationsPerPairForTopic)
        annotPerLangCounter = annotPerLangCounter + annotationsForTopic
        print("\tTOPIC %d/%d : %d , total annotations: %d" % (count_topic, len(topics_per_lang[lang]), topic, annotationsForTopic))
        if not annotationsForTopic:
            empty_topics.append(topic)
            continue
        empty_pairs = []
        for p in range(len(pairs)):
            count_pair = count_pair + 1
            pair = pairs[p]
            annots =  annotations[p]
            print("\t\tpair %d | %d/%d : %s , annotations: %d , annotators: " % (count_pair,p+1, len(pairs), str(pair), len(annots)), end='')
            print("%s" % annots)
            if len(annots) == max_annot:
                complete_pairs.append(pair)

    print ('\tNon annotated topics: %d/%d:  %s' % (len(empty_topics), len(topics_per_lang[lang]), str(empty_topics)))
    print('\tFully annotated pairs: %d  %s' % (len(complete_pairs), str(complete_pairs)))
    print ("Total annotations for %s : %d" % (lang, annotPerLangCounter))
    print()