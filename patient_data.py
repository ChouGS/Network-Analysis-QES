import pandas
import os
import pdb
import shutil
import numpy as np
from matplotlib import pyplot as plot

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

    # the number of minutes past 1900/01/01 00:00
    return date_past * 1440 + hour * 60 + minute


def log(msg: str):
    with open('log.txt', 'a') as f:
        f.write(msg)
    print(msg)


if __name__ == '__main__':
    # if os.path.exists('log.txt'):
    #     os.remove('log.txt')

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

    log(f"{nsurg_patient[1]['count']} patients had 1 surgery.\n"
        f"{nsurg_patient[2]['count']} patients had 2 surgeries.\n"
        f"{nsurg_patient[3]['count']} patients had 3 surgeries.\n"
        f"{nsurg_patient[4]['count']} patients had 4 surgeries.\n\n")


    # Part 3: group visit records by record
    if not os.path.exists('log.txt'):
        patient_site_rec = pandas.read_csv('qes_2022_update/DataBricks/QES_care_sites.csv')
        patient_site_rec = patient_site_rec.loc[patient_site_rec['VISIT_CONCEPT_ID'] == 9201]
        patient_site_rec = patient_site_rec.loc[patient_site_rec['VISIT_END_DATETIME'].notna()]

        site_record_id = patient_site_rec['RECORD_ID']
        site_patient_id = patient_site_rec['PERSON_ID']
        site_visit_id = patient_site_rec['VISIT_OCCURRENCE_ID']

        operation_demo = pandas.read_csv('qes_2022_update/DataBricks/QES_demographics.csv')
        op_rid = operation_demo['RECORD_ID']

        peri_threshold = 90 * 1440
        readmission_threshold = 180 * 1440
        columns = ["RECORD_ID", "PERSON_ID", "VISIT_CONCEPT_ID", "VISIT_OCCURRENCE_ID", "VISIT_START_DATETIME", "VISIT_END_DATETIME", "CARE_SITE_NAME"]

        n_no_record = 0
        n_no_visit = 0
        n_no_peri = 0
        n_no_left = 0
        n_no_right = 0
        n_complete = 0

        rel_time_distribution = []

        frame_all = pandas.DataFrame()

        for record_id in op_rid:

            op_time = operation_demo.loc[operation_demo["RECORD_ID"] == record_id, 'DOS']
            if len(op_time) == 0:
                n_no_record += 1
                print(f'No operation found, skip record {record_id}')
                continue
            op_time = get_timestamp(op_time.iloc[0])

            record_site_frame = patient_site_rec.loc[patient_site_rec["RECORD_ID"] == record_id, columns]
            # record_proc_frame = patient_proc_rec.loc[patient_proc_rec["RECORD_ID"] == record_id]

            if len(record_site_frame['VISIT_OCCURRENCE_ID']) == 0:
                n_no_visit += 1
                print(f'No visit data, skip record {record_id}')
                continue

            record_site_frame['VISIT_START_DATETIME'] = record_site_frame['VISIT_START_DATETIME'].map(get_timestamp)
            record_site_frame['VISIT_END_DATETIME'] = record_site_frame['VISIT_END_DATETIME'].map(get_timestamp)

            record_site_frame['VISIT_DURATION'] = record_site_frame['VISIT_END_DATETIME'] - record_site_frame['VISIT_START_DATETIME']
            
            record_site_frame['REL_START_TIME'] = record_site_frame['VISIT_START_DATETIME'] - op_time
            record_site_frame['REL_END_TIME'] = record_site_frame['VISIT_END_DATETIME'] - op_time

            record_site_frame['IS_PERIOP'] = (record_site_frame['REL_START_TIME'] >= -peri_threshold) * \
                                            (record_site_frame['REL_END_TIME'] <= peri_threshold)
            record_site_frame['IS_READMISSION'] = (record_site_frame['REL_END_TIME'] > peri_threshold) * \
                                                (record_site_frame['REL_START_TIME'] <= readmission_threshold)
            
            if any(record_site_frame['IS_READMISSION'].tolist()):
                record_site_frame['READMITTED'] = 1
            else:
                record_site_frame['READMITTED'] = 0

            if not any(record_site_frame['IS_PERIOP'].tolist()):
                n_no_peri += 1
                print(f'No perioperative data, skip record {record_id}')
                continue

            peri_frame = record_site_frame.loc[record_site_frame['IS_PERIOP'] * (record_site_frame['VISIT_CONCEPT_ID'] == 9201), \
                ["RECORD_ID", "PERSON_ID", "VISIT_OCCURRENCE_ID", "VISIT_START_DATETIME", "VISIT_END_DATETIME", \
                "CARE_SITE_NAME", "REL_START_TIME", "REL_END_TIME", "VISIT_DURATION", "READMITTED"]]

            frame_all = frame_all.append(peri_frame)

            if all((record_site_frame['REL_START_TIME'] > 0).tolist()):
                n_no_left += 1
                print(f'No perioperative data before surgery for record {record_id}')
                # continue

            elif all((record_site_frame['REL_END_TIME'] < 0).tolist()):
                n_no_right += 1
                print(f'No perioperative data after surgery for record {record_id}')
                # continue

            else:
                n_complete += 1
                print(f'Record {record_id} is complete')

            valid_rel_time_frame = record_site_frame.loc[
                record_site_frame['VISIT_START_DATETIME'] * record_site_frame['VISIT_START_DATETIME'] != 0,
                ["REL_START_TIME", "REL_END_TIME"]
            ]

            rel_time_distribution += ((valid_rel_time_frame["REL_START_TIME"] + valid_rel_time_frame["REL_END_TIME"]) / 2).tolist()

            os.makedirs(f'periop_per_qes', exist_ok=True)
            peri_frame.to_csv(f'periop_per_qes/{record_id}-{record2patient[str(record_id)]["Person_ID"]}.csv')


        log(f'{n_no_record} records were not found.\n'
            f'{n_no_visit} records had no visit occurrence.\n'
            f'{n_no_peri} records had no perioperative visits.\n'
            f'{n_no_left} records had no perioperative visits before QES.\n'
            f'{n_no_right} records had no perioperative visits after QES.\n'
            f'{n_complete} records were complete.\n\n')

        frame_all.to_csv('all.csv')

    # Part 4: count the number of care sites in each single visit.
    frame_all = pandas.read_csv('all.csv')
    unique_visit_ids = np.unique(frame_all['VISIT_OCCURRENCE_ID'])

    length_count_dict = {}

    for vid in unique_visit_ids:
        cs_list = []
        last = ''
        visit_frame = frame_all[frame_all['VISIT_OCCURRENCE_ID'] == vid]
        for i in range(len(visit_frame)):
            cs_name = visit_frame['CARE_SITE_NAME'].iloc[i]
            if cs_name != last:
                cs_list.append(cs_name)
                last = cs_name
            
        length = len(cs_list)
        if length not in length_count_dict.keys():
            length_count_dict[length] = 0
        length_count_dict[length] += 1
    
    x = sorted(length_count_dict)
    y = [length_count_dict[k] for k in x]
    z = []
    for i in range(len(x)):
        z += [x[i]] * y[i]
    z = np.array(z)

    log('Statictics for length of visit: (dup_removed)\n'
          f'N:      {len(z)}\n'
          f'Mean:   {np.mean(z).round(3)}\n'
          f'Std:    {np.std(z).round(3)}\n'
          f'Max:    {np.max(z)}\n'
          f'Q75:    {np.quantile(z, 0.75).astype(int)}\n'
          f'Median: {np.median(z).astype(int)}\n'
          f'Q25:    {np.quantile(z, 0.25).astype(int)}\n'
          f'Min:    {np.min(z)}\n\n')

    length_count_dict = {}

    for vid in unique_visit_ids:
        length = len(frame_all.loc[frame_all['VISIT_OCCURRENCE_ID'] == vid, 'CARE_SITE_NAME'].unique())
        if length not in length_count_dict.keys():
            length_count_dict[length] = 0
        length_count_dict[length] += 1
    
    x = sorted(length_count_dict)
    y = [length_count_dict[k] for k in x]
    z = []
    for i in range(len(x)):
        z += [x[i]] * y[i]
    z = np.array(z)

    log('Statictics for # of unique care sites in each visit:\n'
          f'N:      {len(z)}\n'
          f'Mean:   {np.mean(z).round(3)}\n'
          f'Std:    {np.std(z).round(3)}\n'
          f'Max:    {np.max(z)}\n'
          f'Q75:    {np.quantile(z, 0.75).astype(int)}\n'
          f'Median: {np.median(z).astype(int)}\n'
          f'Q25:    {np.quantile(z, 0.25).astype(int)}\n'
          f'Min:    {np.min(z)}\n\n')

    # Part 6: check if one patient appears at different places at the same time
    unique_patient_ids = np.unique(frame_all['PERSON_ID'])
    prob_patients = set()

    for pid in unique_patient_ids:
        patient_record = frame_all.loc[frame_all['PERSON_ID'] == pid, ['VISIT_OCCURRENCE_ID', 'VISIT_START_DATETIME', 'VISIT_END_DATETIME', 'CARE_SITE_NAME']]
        patient_visit_dict = {}
        for i in patient_record.index:
            vtime = (patient_record.loc[i, 'VISIT_START_DATETIME'], patient_record.loc[i, 'VISIT_END_DATETIME'])
            csite = patient_record.loc[i, 'CARE_SITE_NAME']
            if vtime not in patient_visit_dict.keys():
                patient_visit_dict[vtime] = []
            if csite not in patient_visit_dict[vtime]:
                patient_visit_dict[vtime].append(csite)
        for i in patient_visit_dict.keys():
            if len(patient_visit_dict[i]) > 1:
                log(f'Patient {pid} appeared at care sites {patient_visit_dict[i]} at time [{i[0]}, {i[1]}]\n')
                prob_patients.add(pid)
    
    log(f'{len(prob_patients)} unique patients appeared at multiple sites at the same time')
