# Import the libraries

import glob

import os

import pyodbc

import pandas as pd

from dotenv import load_dotenv

# Take Environment Variables from the .env
dotenv_path_str = ".env"

load_dotenv(dotenv_path_str)

server = os.environ.get("SERVER")

database = os.environ.get("DATABASE")

username = os.environ.get("DB_USER")

password = os.environ.get("PASSWORD")

driver= os.environ.get("DRIVER")



# Read the Master List
MASTER_DF = pd.read_csv(r"C:\Users\INNO\Desktop\AI Academy\Students Master List\Business Analytics Users.csv", usecols = ["Azubi Email"])

ALL_MON_DF = []

ALL_WED_DF = []


################################################################################################################
# Change these paths to reflect the new week
FOLDER_PATH = os.path.join("C:/Users/INNO/Desktop/AI Academy/Attendance/Week 3/*")


FINAL_DF = "C:/Users/INNO/Desktop/AI Academy/Attendance/Week 3/computed_df.csv"

DATE_STR = "18/4/2022"

###################################################################################################



    
def create_daily_df(dir_path): 

    global ALL_MON_DF, ALL_WED_DF
    
    for folder in glob.glob(dir_path):
        print(f"\nReading Folder--> {folder}")
    
        if folder.endswith("Monday"):
        
            files_path = os.path.join(folder, "*")
    
            for file in glob.glob(files_path):
                print(f"Reading file path {file}")
    
                try:
                    # Read the excel file and skip till row eight
                    excel_file = pd.read_csv(file, header = 7, usecols = ['Duration', 'Email', 'Participant ID (UPN)'])
        
    
                except UnicodeDecodeError: 
                    excel_file = pd.read_csv(file, header = 6, encoding='UTF-16', sep="\t", usecols = ['Duration', 'Email', 'Participant ID (UPN)'])
  
        
                finally:
                    # Append the Excel file to the All Monday DataFrame
                    ALL_MON_DF.append(excel_file)

            mon_att_df = pd.concat(ALL_MON_DF)

            # Convert Duration to Time Delta
            mon_att_df['Duration'] = pd.to_timedelta(mon_att_df['Duration']).dt.seconds

        
        elif folder.endswith("Wednesday"):
            
            files_path = os.path.join(folder, "*")
    
            for file in glob.glob(files_path):
                print(f"Reading file path {file}")
    
                try:
                    # Read the excel file and skip till row eight
                    excel_file = pd.read_csv(file, header = 7, usecols = ['Duration', 'Email', 'Participant ID (UPN)'])
        
    
                except UnicodeDecodeError: 
                    excel_file = pd.read_csv(file, header = 6, encoding='UTF-16', sep="\t", usecols = ['Duration', 'Email', 'Participant ID (UPN)'])
  
        
                finally:
                    # Append the Excel file to the All Monday DataFrame
                    ALL_WED_DF.append(excel_file)

            wed_att_df = pd.concat(ALL_WED_DF)

            # Convert Duration to Time Delta
            wed_att_df['Duration'] = pd.to_timedelta(wed_att_df['Duration']).dt.seconds    
        
    return mon_att_df, wed_att_df



def create_present_df(monday_df, wednesday_df):  
   
    # Now Due to multiple occurrence of emails, group by email
    mon_att_df_2 = monday_df.groupby('Email')['Duration'].sum().to_frame(name = "Period on Call").reset_index()
    wed_att_df_2 = wednesday_df.groupby('Email')['Duration'].sum().to_frame(name = "Period on Call").reset_index()

    # Create a Present column if Duration > 30 minutes i.e. you are marked present if you attend atleast half of the check-in
    mon_att_df_2['attendance_monday'] = mon_att_df_2['Period on Call'].apply(lambda x: 1 if x > 1800 else 0)
    wed_att_df_2['attendance_wednesday'] = wed_att_df_2['Period on Call'].apply(lambda x: 1 if x > 1800 else 0)

    return mon_att_df_2, wed_att_df_2


def create_final_df(mon_present_df, wed_present_df):
    
    global MASTER_DF, FINAL_DF, DATE_STR
    
    final_df = MASTER_DF.merge(mon_present_df, how = "left", left_on = "Azubi Email", right_on = "Email")
    final_df = final_df.merge(wed_present_df, how = "left", left_on = "Azubi Email", right_on = "Email")

    
    # Replace null value with 0 for the Monday Column
    final_df.loc[(final_df["attendance_monday"].isna()) , ["attendance_monday"]] = 0
    
    # Replace null value with 0 for the Monday Column
    final_df.loc[(final_df["attendance_wednesday"].isna()), ["attendance_wednesday"]] = 0
    
    # Format the DF
    final_df["student_email"] = final_df["Azubi Email"]
    
    final_df["date"] = DATE_STR

    ret_final_df = final_df[["student_email", "date", "attendance_monday", "attendance_wednesday"]]
    
    ret_final_df.to_csv(FINAL_DF, index = False)
    
    return ret_final_df
  
   

      
def main():
    """
    This is the main function
    """
    with pyodbc.connect('DRIVER='+driver+';SERVER=tcp:'+server+';PORT=1433;DATABASE='+database+';UID='+username+';PWD='+ password) as conn:
        
        with conn.cursor() as cursor:
            print("Connection to database established")
            
            mon_df, wed_df = create_daily_df(dir_path = FOLDER_PATH)

            mon_df_2, wed_df_2 = create_present_df(monday_df = mon_df, wednesday_df = wed_df)

            # Create the Computed CSV File
            attendance_df = create_final_df(mon_present_df = mon_df_2, wed_present_df = wed_df_2)
            
            attendance_list = []
            

            for index, row in attendance_df.iterrows():
                
                student_email = row['student_email']
                date = row['date']
                attendance_monday = row['attendance_monday']
                attendance_wednesday = row['attendance_wednesday']
                
                data_tuple = (student_email, date, attendance_monday, attendance_wednesday)
                
                attendance_list.append(data_tuple)
                
            sql_query = """INSERT INTO student_attendance VALUES (?, ?, ?, ?)"""      
        
            cursor.executemany(sql_query, attendance_list)
            
        conn.commit()
        print("Data inserted successfully")  


if __name__ == '__main__':
    main()