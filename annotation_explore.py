
import  os, pickle
from itertools import combinations
import mysql.connector

#################################
# user settings here
# langs_of_interest=['en','el','it','zh']
langs_of_interest = ['el']

credsfile = "mysql.conf"
drop_participants = ["SWAP"]
print_axis = ['quality']
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


    annot_fields = ["annotator_uid","p_first_summary_id","p_second_summary_id"]
    if print_axis:
        annot_fields.extend(print_axis)

    query = ("SELECT %s from annotation" % ", ".join(annot_fields))
    cursor.execute(query)
    annotators = cursor.fetchall()

    query = ("SELECT id, name from participant")
    cursor.execute(query)
    participants = cursor.fetchall()


    query = ("SELECT id, topic_id, participant_id from p_summary")
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
            sumpairs_per_topic[topic] = [[],[],[]] # summary pairs, annotators for said pairs, annotations of axes

        # drop participants - get ids
        if drop_participants is not None:
            drop_ids = [ x[0] for x in participants if x[1] in drop_participants]


        summaries = []
        # get all summaries for topic
        for s in summ:
            if s[1] == topic:
                if s[2] in drop_ids:
                    continue
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
        if print_axis:
            sumpairs_per_topic[topic][2] = [[] for j in range(len(sumpairs))]
        # get annotations per pair of topic
        for ann in annotators:
            pair = [ ann[1], ann[2]]
            pair.sort()
            pair = tuple(pair)
            if pair in sumpairs:
                idx = sumpairs.index(pair)
                sumpairs_per_topic[topic][1][idx].append(ann[0])
                if print_axis:
                    sumpairs_per_topic[topic][2][idx].append(ann[3])

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
    if drop_participants is not None:
        print("Having dropped participants: ",str(drop_participants))
    empty_topics = []
    complete_pairs = []
    for topic in topics_per_lang[lang]:
        count_topic = count_topic  + 1
        pairs_annotations = sumpairs_per_topic[topic]
        pairs = pairs_annotations[0]
        annotators = pairs_annotations[1]
        scores = pairs_annotations[2]
        annotationsPerPairForTopic = list(map(len, annotators))
        annotationsForTopic = sum(annotationsPerPairForTopic)
        annotPerLangCounter = annotPerLangCounter + annotationsForTopic
        print("\tTOPIC %d/%d : %d , total annotations: %d" % (count_topic, len(topics_per_lang[lang]), topic, annotationsForTopic))
        if not annotationsForTopic:
            empty_topics.append(topic)
            continue
        empty_pairs = []
        for p, pair in enumerate(pairs):
            count_pair = count_pair + 1
            annots =  annotators[p]
            score = scores[p]
            print("\t\tpair %d | %d/%d : %s , annotations: %d , annotators: " % (count_pair,p+1, len(pairs), str(pair), len(annots)), end='')
            print("%s" % annots, end='')

            if print_axis is not None:
                print("%s" % str(score),end='')

            # newline
            print()
            if len(annots) == max_annot:
                complete_pairs.append(pair)

    print ('\tNon annotated topics: %d/%d:  %s' % (len(empty_topics), len(topics_per_lang[lang]), str(empty_topics)))
    print('\tFully annotated pairs: %d  %s' % (len(complete_pairs), str(complete_pairs)))
    print ("Total annotations for %s : %d" % (lang, annotPerLangCounter))
    print()
