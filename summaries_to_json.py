 

import  os, pickle
from itertools import combinations
import mysql.connector
import json

def get_database_values(fields, table, cursor):
    query = ("SELECT " + ",".join(fields)  + " from " + table)
    cursor.execute(query)
    values = cursor.fetchall()
    result = []
    for val in values:
        result.append(dict(zip(fields, val)))
    return result

def to_json(gold,summ_a, summ_b, task_descr, choices, id):
    ret = {}
    ret["context"]="<div class=\"gold-summary\"><div style=\"font-weight:bold;\">Gold Summary</div><div>" + gold + "</div></div><div class=\"rTable\"><div class=\"rTableRow\"><div class=\"rTableHead\" style=\"font-weight: bold;\">Candidate summary A</div><div class=\"rTableHead\" style=\"font-weight: bold;\">Candidate summary B</div></div><div class=\"rTableRow\"><div class=\"rTableCell\">" + summ_a + "</div><div class=\"rTableCell\">" + summ_b + "</div></div></div>"

    ret["title"] = "<div>" + task_descr + "</div>"
    ret["choices"] = choices
    ret["priority"] = 5
    ret["members"] = 3
    ret["type"] = "CHOICE"
    ret["id"] = id
    return ret


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
    # get database data
    summ_fields = [ "id", "participant_id", "topic_id", "summary"]
    summaries = get_database_values(summ_fields, "p_summary", cursor)

    refs_fields = ["topic_id", "ref_summary"]
    ref_summaries = get_database_values(refs_fields, "ref_summaries", cursor)

    part_fields = ["id", "name"]
    participants = get_database_values(part_fields , "participant", cursor)

    topic_fields = ["id", "lang_code"]
    topics = get_database_values(topic_fields, "topic", cursor)

    jsonpairsarray = []
    taskobject = {}
    metricdescrs =[
        "Please, consider the following <i>Gold Summary</i> and evaluate the <b>overall responsiveness</b> of the candidate summary A and candidate summary B.<br /><div style=\"font-size:smaller;text-align:left;\"><b>Overall responsiveness:</b> Which of the alternative summary was better, if any, at reflecting the content (information) of the reference summary? <br>Was A better than B (A > B), B better than A (B > A), or were they about the same (A &cong; B). As stated in the<a href=\"http://www-nlpir.nist.gov/projects/duc/duc2007/quality-questions.txt\"> NIST </a>evaluations: <blockquote class=\"small\">     \"Responsiveness should be measured primarily in terms of the AMOUNT OF INFORMATION in the summary that actually helps to satisfy the information...\" </blockquote>",
        "Please, consider the following <i>Gold Summary</i> and evaluate the <b>non redundancy</b> of the candidate summary A and candidate summary B.<br /><div style=\"font-size:smaller;text-align:left;\"><b>Non-Redundancy:</b> Which alternative summary was less redundant? (A > B, B > A, or A &cong; B). As stated in the<a href=\"http://www-nlpir.nist.gov/projects/duc/duc2007/quality-questions.txt\"> NIST </a>evaluations: <blockquote class=\"small\"> \"There should be no unnecessary repetition in the summary. Unnecessary repetition might take the form of whole sentences that are repeated, or repeated facts, or the repeated use of a noun or noun phrase (e.g., \"Bill Clinton\") when a pronoun (\"he\") would suffice.\" </blockquote>",
        "Please, consider the following <i>Gold Summary</i> and evaluate the <b>coherence</b> of the candidate summary A and candidate summary B.<br /><div style=\"font-size:smaller;text-align:left;\"><b>Coherence:</b> Which summary was more coherent? (A > B, B > A, or A &cong; B). As stated in the<a href=\"http://www-nlpir.nist.gov/projects/duc/duc2007/quality-questions.txt\"> NIST </a>evaluations: <blockquote class=\"small\"> \"The summary should be well-structured and well-organized. The summary should not just be a heap of related information, but should build from sentence to sentence to a coherent body of information about a topic.\" </blockquote>",
        "Please, consider the following <i>Gold Summary</i> and evaluate the <b>focus</b> of the candidate summary A and candidate summary B.<br /><div style=\"font-size:smaller;text-align:left;\"><b>Focus:</b>Which summary was more focused in its content, not conveying irrelevant details? (A > B, B > A, or A &cong; B). As stated in the<a href=\"http://www-nlpir.nist.gov/projects/duc/duc2007/quality-questions.txt\"> NIST </a>evaluations: <blockquote class=\"small\"> \"The summary should have a focus; sentences should only contain information that is related to the rest of the summary.\" </blockquote>"
    ]
    metricnames = ["Responsiveness", "Non-Redundancy", "Coherence", "Focus"]
    metricchoices = [{
                    "A is better than B": "A > B",
                    "B is better than A": "B > A",
                    "Cannot really say ": "A ≅ B"
                } for _ in metricnames]
    pairid = 0
    id_log = []
    # generating pairs per language
    lang = "en"

    rel_topics = [t['id'] for t in topics if t['lang_code'] == lang]
    rel_summaries = [s for s in summaries if int(s['topic_id']) in rel_topics]

    paircount = 1

    # get pairs per topic
    for top in rel_topics:
        summs = [s for s in rel_summaries if int(s['topic_id']) == top]
        sumpairs = list(combinations(summs, 2))
        # drop ones with same participant
        sumpairs = [ s for s in sumpairs if not s[0]['participant_id'] == s[1]['participant_id']]

        ref_sum = [ r for r in ref_summaries if r["topic_id"] == top]

        gold = ref_sum[0]["ref_summary"]
        for s in sumpairs:
            if paircount > 2:
                break
            print("id1: %6d | id2: %6d " % (s[0]['participant_id'], s[1]['participant_id']))
            # get ref summary for topic
            text_a = s[0]['summary']
            text_b = s[1]['summary']

            for taskidx in range(len(metricnames)):

                taskname = metricnames[taskidx]
                taskdescr = metricdescrs[taskidx]
                taskchoices = metricchoices[taskidx]

                #(gold, summ_a, summ_b, task_descr, choices, id)
                jsobj = to_json(gold,text_a,text_b,taskdescr,taskchoices,pairid)
                jsonpairsarray.append(jsobj)
                id_log.append([ lang, int(top),int(s[0]['id']), int(s[1]['id']),taskname, pairid ])
                pairid = pairid + 1

            paircount = paircount + 1
        break

    taskobject["tasks"] = jsonpairsarray
    taskobject["name"] = "taskname"
    basename = "%s" % (lang)
    with open("%s.log" % basename,"w") as f:
        f.write(str("lang,topic,summ1,summ2,metric,pair-id") + "\n")
        for l in id_log:
            f.write(str(l) + "\n")
    with open("%s.json" % basename,"w") as f:
        f.write(json.dumps(taskobject, indent=4, ensure_ascii=False))



