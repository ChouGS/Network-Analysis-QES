import pandas
import os
import pdb

def one_one(a, b):
    if len(a) != len(b):
        return False
    rab, rba = (True, True)
    da2b = {}
    for i in range(len(a)):
        if a[i] in da2b.keys():
            if da2b[a[i]] != b[i]:
                rab = False
                break
        else:
            da2b[a[i]] = b[i]

    db2a = {}
    for i in range(len(a)):
        if b[i] in db2a.keys():
            if db2a[b[i]] != a[i]:
                rba = False
                break
        else:
            db2a[b[i]] = a[i]
    
    return rab, rba

def get_timestamp(t: str):
    # Format of t: "yyyy/mm/dd hh:mm"
    # Standard timestamp value: 1900/01/01 00:00 -> 0

    if not t or type(t) is not str or t.lower() == "nan":
        return 0

    ymd, hm = t.split(' ')
    year, month, date = map(lambda x: int(x), ymd.split('/'))
    hour, minute = map(lambda x: int(x), hm.split(':'))

    year_day = [365 for _ in range(1900, 2023)]
    for i in range(4, 122, 4):
        year_day[i] += 1

    month_day = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    date_past = sum(year_day[:year - 1900]) + sum(month_day[:month - 1]) + (date - 1)

    if month > 2 and year_day[year - 1900] == 366:
        date_past += 1

    return date_past * 1440 + hour * 60 + minute

demographic = pandas.read_csv('qes_2022_update/patient_demo_2022.csv')
patient_site_rec = pandas.read_csv('qes_2022_update/DataBricks/QES_care_sites.csv')
patient_proc_rec = pandas.read_csv('qes_2022_update/DataBricks/QES_procedures.csv')
patient_src = pandas.read_csv('qes_2022_update/QES_RECORD_PERSON_MRN_2022_202209282047.csv')
operation_demo = pandas.read_csv('qes_2022_update/DataBricks/QES_demographics.csv')

demo_patient_id = demographic['person_source_value']
demo_record_id = demographic['record_id']

site_patient_id = patient_site_rec['PERSON_ID']
site_record_id = patient_site_rec['RECORD_ID']
site_visit_id = patient_site_rec['VISIT_OCCURRENCE_ID']

proc_patient_id = patient_proc_rec['PERSON_ID']
proc_record_id = patient_proc_rec['RECORD_ID']
proc_visit_id = patient_proc_rec['VISIT_OCCURRENCE_ID']


MRN_src = patient_src['PERSON_SOURCE_VALUE']
MRN_pid = patient_src['PERSON_ID']
MRN_rid = patient_src['RECORD_ID']

op_rid = operation_demo['RECORD_ID']

record2patient = {
    MRN_rid[i]: {"Person_ID": MRN_pid[i], "Person_srcv": MRN_src[i]} for i in range(len(MRN_src))
}

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
    record_site_frame['IS_PERIOP'] = abs(record_site_frame['VISIT_START_DATETIME'] - op_time) <= peri_threshold
    record_site_frame['IS_READMISSION'] = record_site_frame['VISIT_START_DATETIME'] - op_time > peri_threshold

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
