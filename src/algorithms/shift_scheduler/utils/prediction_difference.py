import pandas as pd

def day_count(days_of_year, pessObj, workers, working_shift, schedule_df, cal):

    # Create a DataFrame to store the differences between objective and actual worker counts
    diff_df = pd.DataFrame(index=days_of_year, columns=['M_obj', 'M_actual', 'M_diff', 'T_obj', 'T_actual', 'T_diff'])

    # Fill in the data
    for d in days_of_year:
        for s in working_shift:
            # Get the objective number of workers for this day and shift
            obj_workers = pessObj.get((d, s), 0)
            
            # Count the actual number of workers assigned to this day and shift in the new_schedule_df
            actual_workers = 0
            for w in workers:
                if d < len(schedule_df.columns) - 1:  # Make sure day is within the DataFrame bounds
                    day_col = f"Day {d}"
                    if day_col in schedule_df.columns:
                        worker_row = schedule_df[schedule_df['Worker'] == w]
                        if not worker_row.empty and worker_row[day_col].values[0] == s:
                            actual_workers += 1
            
            # Calculate the difference (negative means shortage, positive means excess)
            diff = actual_workers - obj_workers
            
            # Store values in DataFrame
            diff_df.loc[d, f'{s}_obj'] = obj_workers
            diff_df.loc[d, f'{s}_actual'] = actual_workers
            diff_df.loc[d, f'{s}_diff'] = diff

    # Fill NaN values with 0
    diff_df = diff_df.fillna(0)

    # Convert to appropriate data types
    diff_df = diff_df.astype(int)

    # Add a column for the date for reference
    diff_df['Date'] = cal['DATA'].dt.date.unique()[:len(diff_df)]

    # Reorder columns to have Date first
    cols = ['Date'] + [col for col in diff_df.columns if col != 'Date']
    diff_df = diff_df[cols]
    pd.set_option("display.max_rows", None)
    # Display the results
    print("Worker allocation differences between objective and actual counts:")
    print(diff_df)