def insert_schedule_batch(connection, data):
    """
    Inserts multiple schedule records into wfm.int_schedule_algorithm using executemany.

    Args:
        connection: Active DB connection.
        data: List of tuples, each containing values for one schedule row.

    Returns:
        int: Number of rows inserted or 0 if error.
    """
    query = """
        INSERT INTO wfm.int_schedule_algorithm 
        (FK_PROCESSO, DT_CREATE, EMPLOYEE_ID, SCHEDULE_DT, SCHED_TYPE, SCHED_SUBTYPE,
         START_TIME_1, END_TIME_1, START_TIME_2, END_TIME_2,
         OPTION_TYPE, OPTION_C1, OPTION_N1)
        VALUES (:1, sysdate, :2, to_date(:3, 'YYYY-MM-DD'), :4, :5, :6, :7, :8, :9, :10, :11, :12)
    """
    try:
        with connection.cursor() as cursor:
            cursor.executemany(query, data)
        connection.commit()
        return len(data)
    except Exception as e:
        print(f"Error inserting batch schedule: {e}")
        return 0
