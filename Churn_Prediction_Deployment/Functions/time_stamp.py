from datetime import datetime, timedelta
import pandas as pd

def get_yesterdays_timestamps(backfill_mode, start_date_str=None, end_date_str=None):
    if start_date_str and end_date_str:
        start_date = datetime.strptime(start_date_str, '%Y-%m-%d') - timedelta(hours=5, minutes=30)
        end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(hours=18, minutes=30)
        start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
    current_time_utc = datetime.now()
    previous_day_start = current_time_utc.replace(hour=18, minute=30, second=0, microsecond=0) - timedelta(days=2)

    previous_day_end = current_time_utc.replace(hour=18, minute=30, second=0, microsecond=0) - timedelta(days=1)

    previous_day_start_time = previous_day_start.strftime("%Y-%m-%d %H:%M:%S")
    previous_day_end_time = previous_day_end.strftime("%Y-%m-%d %H:%M:%S")
    
    if backfill_mode == 'live':
        return previous_day_start_time, previous_day_end_time
    else:
        return start_date_str, end_date_str

def get_timestamps(backfill_mode, timezone, start_date_str=None, end_date_str=None):
    if timezone=='IST':
        if start_date_str and end_date_str:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d') - timedelta(hours=5, minutes=30)
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d') + timedelta(hours=18, minutes=30)
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
    
        current_time_utc = datetime.now()
        if current_time_utc.time() < datetime.strptime('18:30:00', '%H:%M:%S').time():
            previous_day_start = current_time_utc.replace(hour=18, minute=30, second=0, microsecond=0) - timedelta(days=1)
            hour_start_time = previous_day_start
        else:
            current_day_start = current_time_utc.replace(hour=18, minute=30, second=0, microsecond=0)
            hour_start_time = current_day_start

        ist_time = current_time_utc + timedelta(minutes=330)
        end_time_ist = ist_time.replace(minute=0, second=0, microsecond=0)
        start_time_ist = end_time_ist - timedelta(hours=1)
        previous_hour_start_time_ist = start_time_ist- timedelta(minutes=330)
        previous_hour_end_time_ist = end_time_ist-timedelta(minutes=330) - timedelta(seconds=1)
        previous_day_start_time_ist=hour_start_time.strftime("%Y-%m-%d %H:%M:%S")

        s_previous_day_start_time = hour_start_time - timedelta(days=1)
        s_previous_day_end_time = hour_start_time
        ns_previous_day_end_time = hour_start_time + timedelta(hours=3)

        s_previous_day_start_time_str = s_previous_day_start_time.strftime("%Y-%m-%d %H:%M:%S")
        s_previous_day_end_time_str = s_previous_day_end_time.strftime("%Y-%m-%d %H:%M:%S")
        ns_previous_day_end_time_str = ns_previous_day_end_time.strftime("%Y-%m-%d %H:%M:%S")
    
        if backfill_mode == 'live':
            return previous_day_start_time_ist,previous_hour_start_time_ist, previous_hour_end_time_ist
        elif backfill_mode == 'daily_backfill':
            return s_previous_day_start_time_str, s_previous_day_end_time_str, ns_previous_day_end_time_str
        else:
            return start_date_str, end_date_str
    
    elif timezone=='CDT':
        current_time_utc = datetime.now()
        cdt_time = current_time_utc - timedelta(minutes=300)
        end_time_cdt = cdt_time.replace(minute=0, second=0, microsecond=0)
        start_time_cdt = end_time_cdt - timedelta(hours=1)
        previous_hour_start_time_cdt = start_time_cdt+ timedelta(minutes=300)
        previous_hour_end_time_cdt = end_time_cdt+timedelta(minutes=300) - timedelta(seconds=1)
        if backfill_mode == 'live':
            return previous_hour_start_time_cdt,previous_hour_end_time_cdt

def get_previous_week_timestamps():
    today = datetime.date.today()
    current_week_start = today - datetime.timedelta(days=today.weekday())
    previous_week_start = current_week_start - datetime.timedelta(days=7)
    previous_week_end = current_week_start - datetime.timedelta(days=1)

    previous_week_start_timestamp = pd.Timestamp(previous_week_start)
    previous_week_end_timestamp = pd.Timestamp(previous_week_end).replace(hour=23, minute=59, second=59)

    offset = datetime.timedelta(minutes=330)
    previous_week_start_timestamp_adj = previous_week_start_timestamp - offset
    previous_week_end_timestamp_adj = previous_week_end_timestamp - offset
    
    return previous_week_start_timestamp, previous_week_end_timestamp, previous_week_start_timestamp_adj, previous_week_end_timestamp_adj

def get_date_330_minutes_before_start_of_month():
    now = datetime.now()
    start_of_month = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    interval = timedelta(seconds=330 * 60)
    result_date = start_of_month - interval
    return result_date