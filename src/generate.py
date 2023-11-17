import sys
import pandas as pd
import sqlite3
from datetime import datetime, timedelta

def get_opening_hours_for_store(store_id, cursor):
    query = f'Select day, start_time_local, end_time_local from menu_hours where store_id = ?'
    cursor.execute(query, (store_id,))
    result = cursor.fetchall()
    if len(result) != 0:
        return result
    default_opening = []
    for i in range(7):
        default_opening.append((i, '00:00:00', '23:59:59'))
    return default_opening

def get_store_status_for_store(store_id, last_timestamp, cursor):
    query = 'Select * from store_status where store_id = ? and timestamp_utc >= ?'
    cursor.execute(query, (store_id, last_timestamp))
    return cursor.fetchall()

def get_timezone_offset_for_store(store_id, cursor):
    query = 'Select timezone_offset from store_timezone where store_id = ?'
    cursor.execute(query, (store_id,))
    result = cursor.fetchone()
    if result is not None:
        return result[0]
    else:
        return -21600
def get_current_max_timestamp(cursor):
    query = 'SELECT MAX(timestamp_utc) from store_status'
    cursor.execute(query)
    return cursor.fetchone()[0]

def get_uptime_downtime(stat_start_time, stat_end_time, store_status, opening_hours, current_time_epoch, timezone_offset, unit='MINUTE'):
    """
        @stat_start_time: e.g current time - 1 day for last_day status
        @stat_end_time: current time
        @store_status: all status entry for the store
        @opening_hours: menu hours of the store
        @current_time_epoch: current timestamp epoch
        @timezone_offset: timezone_offset in seconds
    """
    # print(f'{stat_start_time} {stat_end_time}')
    last_status = []
    # considering only status entry having entry time > stat_start_time
    for each in store_status:
        temp = {}
        temp['status'] = each[2]
        dt = datetime.utcfromtimestamp(each[1] + timezone_offset)
        if dt >= stat_start_time:
            temp['timestamp'] = dt
            temp['left'] = 30 # 30 minute left inactive
            temp['right'] = 30 # 30 minute right inactive
            last_status.append(temp)
    last_status = sorted(last_status, key=lambda x: x['timestamp'])

    # mean interpolation
    if len(last_status) > 1:
        for i in range(1, len(last_status)):
            current = last_status[i]
            prev = last_status[i-1]
            diff = (current['timestamp'] - prev['timestamp']).total_seconds()/60
            prev['right'] = diff/2
            current['left'] = diff/2
            
    # [print(x) for x in last_status]
    opening_hours_stru = []
    # Modify opening hours timing based upon 'stat_start_time' and 'stat_end_time'
    for each in opening_hours:
        opening_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset)
        closing_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset)

        # for opening datetime, date equal to curretnt time but time equal to opening time
        opening_time = opening_time.replace(hour=int(each[1][0:2]), minute=int(each[1][3:5]), second=int(each[1][6:8]))

        # for closing datetime, date equal to curretnt time but time equal to opening time
        closing_time = opening_time.replace(hour=int(each[2][0:2]), minute=int(each[2][3:5]), second=int(each[2][6:8]))
        
        # adjust the date for weekday other than current timestamp weekday
        opening_time = opening_time + timedelta(days=each[0] - opening_time.weekday())
        closing_time = closing_time + timedelta(days=each[0] - closing_time.weekday())

        # consider only valid opening hours
        temp = {}
        if closing_time > stat_start_time and opening_time < stat_end_time:
            temp['opening_time'] = max(opening_time, stat_start_time)
            temp['closing_time'] = min(closing_time, stat_end_time)
            opening_hours_stru.append(temp)

    opening_hours_stru = sorted(opening_hours_stru, key=lambda x: x['opening_time'])
    # [print(x) for x in opening_hours_stru]
    total = 0
    downtime = 0
    for opening_hour in opening_hours_stru:
        opening_time = opening_hour['opening_time']
        closing_time = opening_hour['closing_time']
        for current in last_status:
            # asuming each inactive to be a duration of one hour 30 minute left and 30 minute right to status timestamp
            current_timestamp = current['timestamp'] # status timestamp in local
            inactive_start = current_timestamp - timedelta(minutes=current['left']) # asuming inactive 30 minute before
            inactive_end = current_timestamp + timedelta(minutes=current['right']) # asuming inactive 30 minute after

            # for inactive calculating downtime
            if current['status'] == 'inactive':
                overlap_start = max(opening_time, inactive_start)
                overlap_end = min(closing_time, inactive_end)
                if overlap_start < overlap_end:
                    downtime += (overlap_end - overlap_start).total_seconds()
                    # print('downtime:', downtime/60, 'total:', total/60)
            
        total += (closing_time - opening_time).total_seconds()
    uptime = total - downtime
    converter = 1
    if unit == 'MINUTE':
        converter = 60
    elif unit == 'HOUR':
        converter = 3600
    return int(uptime/converter), int(downtime/converter)
    
def analysis_for_store(store_id, index, cursor):
    # print(f'{index} Analyzing for store_id: {store_id}')
    # current time epoch for now max of store_status timestamp_utc
    current_time_epoch = get_current_max_timestamp(cursor)
    timezone_offset = get_timezone_offset_for_store(store_id, cursor)
    
    # stat end time will be the current time but in local time
    stat_end_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset)

    # get all the status for a particalar store_id
    store_status = get_store_status_for_store(store_id, current_time_epoch - 24*7*3600, cursor)

    # sort status record in ascending order based upon reco
    store_status = sorted(store_status, key= lambda x: x[1])


    last_hour_start_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset - 3600)
    last_day_start_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset - 24*3600)
    last_week_start_time = datetime.utcfromtimestamp(current_time_epoch + timezone_offset - 7*24*3600)

    # store opening hours
    opening_hours = get_opening_hours_for_store(store_id, cursor)

    # calculate uptime and downtime
    uptime_last_hour, downtime_last_hour = get_uptime_downtime(last_hour_start_time, stat_end_time, store_status, opening_hours, current_time_epoch, timezone_offset, 'MINUTE')
    uptime_last_day, downtime_last_day = get_uptime_downtime(last_day_start_time, stat_end_time, store_status, opening_hours, current_time_epoch, timezone_offset, 'HOUR')
    uptime_last_week, downtime_last_week = get_uptime_downtime(last_week_start_time, stat_end_time, store_status, opening_hours, current_time_epoch, timezone_offset, 'HOUR')

    output = {
        'store_id': store_id,
        'uptime_last_hour': uptime_last_hour,
        'uptime_last_day': uptime_last_day,
        'uptime_last_week': uptime_last_week,
        'downtime_last_hour': downtime_last_hour,
        'downtime_last_day': downtime_last_day,
        'downtime_last_week': downtime_last_week
    }
    # print(output)
    return output

def update_report_status(report_id, cursor):
    update_query = "UPDATE store_report SET status = ?, output_filename = ? WHERE report_id = ?;"
    cursor.execute(update_query, ('Complete', f'{report_id}.csv', report_id))
    conn.commit()

def fetch_distinct_store(cursor):
    query = 'SELECT distinct(store_id) from store_status'
    cursor.execute(query)
    result = cursor.fetchall()
    # return ['1000400335282361328']
    return [x[0] for x in result]

if __name__ == '__main__':
    # report id will be passed from /trigger_report api
    report_id = sys.argv[1] if len(sys.argv) > 1 else 'output'

    # create database connection
    conn = sqlite3.connect('data/database.db')
    cursor = conn.cursor()

    # fetch all dinstict store_id
    all_store = fetch_distinct_store(cursor)
    length = len(all_store)
    output = []
    for i in range(len(all_store[0:length])):
        r = analysis_for_store(all_store[i], i, cursor)
        output.append(r)
    
    # create dataframe 
    df = pd.DataFrame(output)

    # output filename 
    csv_file_path = f'output/{report_id}.csv'

    # save file for reuse purpose
    df.to_csv(csv_file_path, index=False)

    # update report status to complete
    update_report_status(report_id, cursor)
    print(f'Output file generated {csv_file_path}')

    # close database connection
    conn.close()
