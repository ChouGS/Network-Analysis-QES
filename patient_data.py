import pandas
import os
import pdb
import json

'''
    Getting started: 
        - download the "qes_2022_update" folder from Box and unzip it to the same
          directory of this script. Make sure your directory tree looks like this:
          ├ patient_data.py
          └ qes_2022_update
            ├ DataBricks
            └ xxx.csv
        - get the pandas and json packages ready

    About the qes data:
        Entities in the qes data:
            - SURGERY(RECORD),  indexed by RECORD_ID
            - PATIENT,          indexed by PERSON_ID or PERSON_SOURCE_VALUE
            - VISIT,            indexed by VISIT_OCCURRENCE_ID
            - CARE_SITE         mapped by CARE_SITE_CONCEPT_NAME
        Relationships among the entities:
            - Each SURGERY is represented in the dataset as one RECORD
            - Each RECORD is linked to 1 PATIENT; each patient may have multiple RECORDs linked
            - Each RECORD is composed of multiple VISITs; each VISIT belongs to exactly one RECORD
            - Each VISIT contains a series of CARE_SITEs; one CARE_SITE can be included in different CARE_SITEs
        Important attributes for building our cohort:
            - Each RECORD has a DOS, that's the date the PATIENT received the qes surgery.
            - Each VISIT has a start time VISIT_START_DATETIME and an end time VISIT_END_DATETIME.
              We will label each VISIT as perioperative if:
                  DOS - VISIT_START_TIME <= 3 months, or VISIT_END_TIME - DOS <= 3 months
              or non-perioperative otherwise.
              We will label each VISIT as readmission if:
                  3 months < VISIT_END_TIME - DOS < 6 months
              or non-readmission otherwise.  

    This script groups qes data into separate files grouped by RECORDs, and
    attempts to make as much statistic as possible of the data.
'''

def one_one(a, b):
    '''
        Check if paired elements in iterables a and b have a one-to-one mapping.
        a and b are expected to have same length.
    '''
    if len(a) != len(b):
        return False, False, None, None
    
    # o2o_ab: whether each element in a can be mapped to exactly one element in b
    # o2o_ba: whether each element in b can be mapped to exactly one element in a
    o2o_ab, o2o_ba = (True, True)   
    
    # dict_ab: the exact mapping from elements in a to elements in b
    dict_ab = {}
    for i in range(len(a)):
        if str(a[i]) in dict_ab.keys():
            if b[i] not in dict_ab[str(a[i])]:
                o2o_ab = False
                dict_ab[str(a[i])].append(b[i])
        else:
            dict_ab[str(a[i])] = [b[i]]

    # dict_ba: the exact mapping from elements in b to elements in a
    dict_ba = {}
    for i in range(len(b)):
        if str(b[i]) in dict_ba.keys():
            if a[i] not in dict_ba[str(b[i])]:
                o2o_ba = False
                dict_ba[str(b[i])].append(a[i])
        else:
            dict_ba[str(b[i])] = [a[i]]
    
    return o2o_ab, o2o_ba, dict_ab, dict_ba

def get_timestamp(t: str):
    '''
        Converts a formatted time string t to a integer timestamp value.
        Format of t: "yyyy/mm/dd hh:mm"
        Standard timestamp value: 1900/01/01 00:00 has the timestamp 0,
        for every minute later than that the timestamp increases by 1.
    '''

    # Skip unavailable time values
    if not t or type(t) is not str or t.lower() == "nan":
        return 0

    # year, month, date, hour, minute values
    ymd, hm = t.split(' ')
    year, month, date = map(lambda x: int(x), ymd.split('/'))
    hour, minute = map(lambda x: int(x), hm.split(':'))

    # the number of days past 1900/01/01
    year_day = [365 for _ in range(1900, 2023)]
    for i in range(4, 122, 4):
        year_day[i] += 1

    month_day = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    date_past = sum(year_day[:year - 1900]) + sum(month_day[:month - 1]) + (date - 1)

    if month > 2 and year_day[year - 1900] == 366:
        date_past += 1

    # the nnumber of minutes past 1900/01/01 00:00
    return date_past * 1440 + hour * 60 + minute


if __name__ == '__main__':

    # Part 1: Build mapping among RECORD_ID, PERSON_ID and PERSON_SOURCE_VALUE
    patient_src = pandas.read_csv('qes_2022_update/QES_RECORD_PERSON_MRN_2022_202209282047.csv')

    MRN_src = patient_src['PERSON_SOURCE_VALUE'].map(str)
    MRN_pid = patient_src['PERSON_ID'].map(str)
    MRN_rid = patient_src['RECORD_ID'].map(str)

    record2patient = {
        MRN_rid[i]: {"Person_ID": MRN_pid[i], "Person_srcv": MRN_src[i]} for i in range(len(MRN_rid))
    }
    src2id = {MRN_src[i]: MRN_pid for i in range(len(MRN_src))}
    id2src = {MRN_pid[i]: MRN_src for i in range(len(MRN_pid))}

    # Part 2: does each patient receive exactly one qes surgery?
    # Read patient informations
    patient_demo = pandas.read_csv('qes_2022_update/patient_demo_2022.csv')

    demo_patient_id = patient_demo['person_source_value']
    demo_record_id = patient_demo['record_id']

    o2o_ab, _, dict_ab, _ = one_one(demo_patient_id, demo_record_id)
    
    if not o2o_ab:
        # some of the patients had more than 1 qes surgery.
        patient_nsurg = {k: len(dict_ab[k]) for k in dict_ab.keys()}

        nsurg_patient = {}
        for k in patient_nsurg.keys():
            if patient_nsurg[k] not in nsurg_patient.keys():
                nsurg_patient[patient_nsurg[k]] = {'count': 0, 'patients': []}
            nsurg_patient[patient_nsurg[k]]['count'] += 1
            nsurg_patient[patient_nsurg[k]]['patients'].append(k)

    pdb.set_trace()

    

    exit()

    patient_site_rec = pandas.read_csv('qes_2022_update/DataBricks/QES_care_sites.csv')
    operation_demo = pandas.read_csv('qes_2022_update/DataBricks/QES_demographics.csv')

    site_patient_id = patient_site_rec['PERSON_ID']
    site_record_id = patient_site_rec['RECORD_ID']
    site_visit_id = patient_site_rec['VISIT_OCCURRENCE_ID']


    op_rid = operation_demo['RECORD_ID']


    peri_threshold = 90 * 1440
    columns = ["VISIT_OCCURRENCE_ID", "VISIT_START_DATETIME", "VISIT_END_DATETIME", "CARE_SITE_NAME"]

    for record_id in op_rid:

        op_time = operation_demo.loc[operation_demo["RECORD_ID"] == record_id, 'DOS']
        if len(op_time) == 0:
            print(f'No operation found, skip record {record_id}')
            continue
        op_time = get_timestamp(op_time.iloc[0])

        record_site_frame = patient_site_rec.loc[patient_site_rec["RECORD_ID"] == record_id, columns]
        # record_proc_frame = patient_proc_rec.loc[patient_proc_rec["RECORD_ID"] == record_id]

        if len(record_site_frame['VISIT_OCCURRENCE_ID']) == 0:
            print(f'No visit data, skip record {record_id}')
            continue

        record_site_frame['VISIT_START_DATETIME'] = record_site_frame['VISIT_START_DATETIME'].map(get_timestamp)
        record_site_frame['VISIT_END_DATETIME'] = record_site_frame['VISIT_END_DATETIME'].map(get_timestamp)

        if min(abs(record_site_frame['VISIT_START_DATETIME'] - op_time) > peri_threshold):
            print(f'No periop data, skip record {record_id}')
            continue

        record_site_frame['VISIT_DURATION'] = record_site_frame['VISIT_END_DATETIME'] - record_site_frame['VISIT_START_DATETIME']
        record_site_frame['IS_PERIOP'] = (op_time - record_site_frame['VISIT_START_DATETIME'] <= peri_threshold) or\
                                        (record_site_frame['VISIT_START_DATETIME'] - op_time <= peri_threshold)
        record_site_frame['IS_READMISSION'] = record_site_frame['VISIT_END_DATETIME'] - op_time > peri_threshold

        perioperative_visits = record_site_frame[record_site_frame['IS_PERIOP'] == True]

        visit_id = 0
        vid_unique = sorted(list(set(record_site_frame['VISIT_OCCURRENCE_ID'].to_list())))

        os.makedirs(f'per_operation/{record_id}-{record2patient[record_id]["Person_ID"]}', exist_ok=True)
        perioperative_visits.to_csv(f'per_operation/{record_id}-{record2patient[record_id]["Person_ID"]}/peri.csv')

        for visit in vid_unique:
            visit_data = record_site_frame[record_site_frame['VISIT_OCCURRENCE_ID'] == visit]
            visit_data.to_csv(f'per_operation/{record_id}-{record2patient[record_id]["Person_ID"]}/{visit_id}-{visit}.csv')
            visit_id += 1
        
        print(f'Record {record_id} saved')
