from datetime import datetime, timedelta

def helper_get_data_range_fact_table(data_range, **kwargs):
    #access dag execution_date
    end_date = kwargs.get('execution_date').date()
    #backfill condition
    actual_execute_date = datetime.now().date()
    if actual_execute_date - end_date <= timedelta(days=1):
        current_date = end_date - timedelta(days=7)
    else:
        current_date = end_date
    date_format = '%Y-%m-%d'
    data_dates = []
    while current_date <= end_date:
        current_date += timedelta(days=1)
        data_dates.append(current_date.strftime(date_format))
    return data_dates

def helper_process_data(data: str):
    # Processing data based on extracted data
    print(f"Processing: {data}")
