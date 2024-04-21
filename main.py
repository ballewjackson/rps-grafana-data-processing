import pandas as pd
from sheetsInterface import get_timestamp_column, get_time_course_data, get_print_batch_data_kg_by_course
import datetime

semesterDates = (
    ("2023-08-20", "2023-12-09", "23_Fall"),
    ("2024-01-14", "2024-05-5", "24_Spring")
)

def get_week_and_semester_number(timestamp, semester_dates):
    """
    Classifies a given timestamp into a week number and semester tag based on the semester_dates input. 
    """
    for start_date_str, end_date_str, semester_tag in semester_dates:
        try:
            start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
            end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
            
            if start_date <= timestamp <= end_date:
                date_diff = timestamp - start_date
                week_number = date_diff.days // 7 + 1
                return week_number, semester_tag
        except ValueError:
            print(f"Invalid date format: {start_date_str}, {end_date_str}")    
    return None, None

# Exclude future weeks for the current semester
def filter_future_weeks(row):
    """
    Filters out rows that are in the future for the current semester.
    """
    current_date = datetime.datetime.now().date()
    for start_date_str, end_date_str, semester_tag in semesterDates:
        start_date = datetime.datetime.strptime(start_date_str, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_date_str, "%Y-%m-%d").date()
        if row['Semester'] == semester_tag and current_date < end_date:
            current_week_number, _ = get_week_and_semester_number(current_date, semesterDates)
            return row['Week'] <= current_week_number
    return True

def save_orders_per_week():
    """
    Saves the orders per week data to a CSV file. Check the CSV to see what this looks like.
    """
    df = get_timestamp_column()

    df['date'] = df['Timestamp'].apply(lambda x: x.split()[0])
    df['date'] = pd.to_datetime(df['date']).dt.date

    # Takes the date column and applies the classifier to it. The * unpacks the returned tuple into 'Week' and 'Semester'.
    df['Week'], df['Semester'] = zip(*df['date'].apply(lambda x: get_week_and_semester_number(x, semesterDates)))

    # Remove rows with NaN values in the 'Week' column
    df = df.dropna(subset=['Week'])
    df['Week'] = df['Week'].astype(int)
    df['Semester'] = df['Semester'].fillna(0)

    df['Include'] = df.apply(filter_future_weeks, axis=1)
    df = df[df['Include']]

    orders_per_week = df.groupby(['Semester', 'Week']).size().reset_index(name='Order Count')

    # Pivot the DataFrame to have semesters as columns
    pivoted_data = orders_per_week.pivot(index='Week', columns='Semester', values='Order Count').fillna(0)

    # Reset index to make 'Week' a column again
    pivoted_data.reset_index(inplace=True)

    # delete week 17 - I addressed this by shifting the end date by 1
    # pivoted_data = pivoted_data[pivoted_data['Week'] != 17]

    # Save the pivoted DataFrame to a single CSV file
    pivoted_data.to_csv('orders_per_week.csv', index=False)

    print("Successfully saved orders per week to orders_per_week.csv")

def save_orders_per_course():
    """
    Saves the number of orders for each course in each week of the semester to a CSV file. The count is averaged between semesters.
    """
    df = get_time_course_data()

    df['date'] = df['Timestamp'].apply(lambda x: x.split()[0])
    df['date'] = pd.to_datetime(df['date']).dt.date
    df['Week'], df['Semester'] = zip(*df['date'].apply(lambda x: get_week_and_semester_number(x, semesterDates)))

    # We want the overage order number per week for each course, take the average of the different semesters
    orders_per_week_semester = df.groupby(['Course', 'Semester', 'Week']).size().reset_index(name='Order Count')
    average_order_count_per_week = orders_per_week_semester.groupby(['Course', 'Week'])['Order Count'].mean().reset_index()
    # need to pivot so that each class is a column
    pivoted_data = average_order_count_per_week.pivot(index='Week', columns='Course', values='Order Count').fillna(0)
    pivoted_data.to_csv('orders_per_course.csv', index=True)
    print("Successfully saved orders per course to orders_per_course.csv")

def save_kg_per_course():
    """
    Saves the mass per course for each semester to CSV files. Also saves the total mass printed up to each week for both semesters.
    """
    df = get_print_batch_data_kg_by_course()

    df['date'] = df['Timestamp'].apply(lambda x: x.split()[0])
    df['date'] = pd.to_datetime(df['date']).dt.date

    df['Week'], df['Semester'] = zip(*df['date'].apply(lambda x: get_week_and_semester_number(x, semesterDates)))

    df = df.dropna(subset=['Week'])
    df['Week'] = df['Week'].astype(int)
    df['Semester'] = df['Semester'].fillna(0)

    # now we want to format the dataframe such that we have a week column and a column for each course
    # the data value in each row should be the sum of the mass values for that course on that week
    # for now only take the data for Semester = 23_Fall
    key1 = "23_Fall"
    df2 = df[df['Semester'] == key1]
    df2 = df2.drop(columns=['Timestamp', 'date', 'Semester'])
    df2 = df2.groupby(['Course', 'Week']).sum().reset_index()
    df2 = df2.pivot(index='Week', columns='Course', values='Mass (kg)').fillna(0)
    df2.reset_index(inplace=True)
    # print(df.head())
    for course in df2.columns[1:]:
        df2[course] = df2[course].astype(float)
    
    df2.to_csv(f"mass_per_course_{key1}.csv", index=False)
    print(f"Successfully saved mass per course to mass_per_course_{key1}.csv")

    # print df2, columns Week, MEEN 361
    # print(df2[['Week', 'MEEN 361']])

    key2 = "24_Spring"
    df3 = df[df['Semester'] == key2]
    df3 = df3.drop(columns=['Timestamp', 'date', 'Semester'])
    df3 = df3.groupby(['Course', 'Week']).sum().reset_index()
    df3 = df3.pivot(index='Week', columns='Course', values='Mass (kg)').fillna(0)
    df3.reset_index(inplace=True)
    
    all_weeks_df = pd.DataFrame({'Week': range(1, 17)})
    df3 = pd.merge(all_weeks_df, df3, on='Week', how='left').fillna(0)
    # convert all values to floats
    for course in df3.columns[1:]:
        df3[course] = df3[course].astype(float)
    
    df3.to_csv(f"mass_per_course_{key2}.csv", index=False)
    print(f"Successfully saved mass per course to mass_per_course_{key2}.csv")
    # print(df3)

    # using df2 and df3, get the total print volume up to the week for each semester in a single dataframe
    # for each course, sum the mass for all weeks up to the current week
    for course in df2.columns[1:]:
        df2[course] = df2[course].cumsum()
    for course in df3.columns[1:]:
        df3[course] = df3[course].cumsum()

    # print(df3)
    
    # We want the total not per course but of all the courses
    df2['Total'] = df2.sum(axis=1)
    df3['Total'] = df3.sum(axis=1)

    # now combine the dataframes
    # df4 = pd.merge(df2, df3, on='Week', how='outer')
    df4 = pd.merge(df2, df3, on='Week', how='outer', suffixes=('_23_Fall', '_24_Spring'))
    # print(df4.columns)
    # Returns: Index(['Week', 'MEEN 210_x', 'MEEN 305_x', 'MEEN 344', 'MEEN 345_x',
    #   'MEEN 361_x', 'MEEN 368', 'MEEN 401_x', 'MEEN 402_x', 'MEEN 404_x',
    #  'MEEN 408', 'MEEN 431', 'MEEN 433', 'MEEN 439_x', 'MEEN 442',
    #  'MEEN 445', 'MEEN 489', 'MEEN 612', 'MEEN 645', 'MEEN 667', 'MEEN 688',
    #  'MEEN 689', 'Total_x', 'MEEN 210_y', 'MEEN 225', 'MEEN 305_y',
    #  'MEEN 345_y', 'MEEN 361_y', 'MEEN 401_y', 'MEEN 402_y', 'MEEN 404_y',
    #  'MEEN 439_y', 'MEEN 460', 'MEEN 485', 'Total_y'],
    # dtype='object')
    # drop all of the course codes
    df4 = df4.drop(columns=[col for col in df4.columns if 'MEEN' in col])
    df4.to_csv("semester_mass_upto_week.csv", index=False)
    print("Successfully saved semester print mass upto week to semester_mass_upto_week.csv")

save_orders_per_week()
save_orders_per_course()
save_kg_per_course()
