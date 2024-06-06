import argparse
import pandas as pd
from pymongo import MongoClient
import matplotlib.pyplot as plt
from pandas.plotting import table
from tkinter import Tk, Frame
from pandastable import Table, TableModel
import warnings
from datetime import datetime, timedelta
import json
from bson import ObjectId, Timestamp
from pandas import Timestamp
from tabulate import tabulate
import os
#the amount of imports u can tell i tried everything lmao
# Get terminal size (SOME NICE CUSTOMIZATION I FOUND TO ADJUST TERMINAL WITH THE DATA)
terminal_width = os.get_terminal_size().columns

# Number of columns 
num_columns = 9  #just count the columns 

# max width per column, leaving some margin for index and extra space
max_width_per_column = max(int(terminal_width / num_columns) - 5, 10)

# display settings
pd.set_option('display.max_columns', None)  # all columns
pd.set_option('display.max_rows', None)  # all rows
pd.set_option('display.width', terminal_width)  
pd.set_option('display.max_colwidth', max_width_per_column)  #maximum column width


#fuck warnings im just trynna build a damn table for my own reference
warnings.filterwarnings("ignore", category=FutureWarning)



def display_data_with_pandastable(data):
    # Create the main Tkinter window
    root = Tk()
    root.title('Data')
    
    #  Frame as a container in the root window
    frame = Frame(root)
    frame.pack(fill='both', expand=True)

    # pandastable Table within the Frame
    pt = Table(frame, dataframe=data, showtoolbar=True, showstatusbar=True)
    pt.show()

    #Tkinter event loop
    root.mainloop()
#every argument deals with its respective purpose I added some extra arguments to make the code a little bit more functional in order to use the --user input --first --second --third in --mega or --weekly separately 
def parse_arguments():
    parser = argparse.ArgumentParser(description='process and insert QA data into MongoDB.  specific criteria. to follow')
    parser.add_argument('--insert', type=str, help='path to the Excel file containing QA data.')
    parser.add_argument('--date', type=str, help='all reports on a specific build date MM/DD/YYYY from both collections')
    parser.add_argument('--weekly', action='store_true', help='fetch data from collection 1 for weekly reports')
    parser.add_argument('--first', action='store_true', help='Report the very first test case')
    parser.add_argument('--middle', action='store_true', help='Report the middle test case')
    parser.add_argument('--last', action='store_true', help='Report the final test case')
    parser.add_argument('--mega', action='store_true', help='Fetch all data from collection 2 as a reference')
    parser.add_argument('--user', type=str, help='all reports associated with a specific user from both collections')
    parser.add_argument('--export-csv', action='store_true', help='Export fetched user data to CSV or any other argument that needs to be saved in the data')
    parser.add_argument('--repeatables', action='store_true', help='List all repeatable bugs from both collections (No duplicates)')
    parser.add_argument('--blocker', action='store_true', help='List all Blocker bugs from both collections (No duplicates)')
    return parser.parse_args()


def connect_to_database():
    client = MongoClient('localhost', 27017)  
    db = client.my_database
    return db

def read_and_process_file(filename):
    data = pd.read_excel(filename)  # Load the Excel file
    
    # Remove duplicates
    initial_count = len(data)
    data.drop_duplicates(inplace=True)
    
    # Remove rows with empty cells
    data.dropna(inplace=True)
    #state difference for collection 1 cleaning and specific second file inserted (as i said my data is all wrong :( )
    if "EG4-DBDump" in filename:
        data = data[pd.to_numeric(data['Test #'], errors='coerce').notnull()]
        data['Test #'] = data['Test #'].astype(int)
        #data = data[data['Test #'] != 0]
    
    # Validate 'Build #' date format ("M/D/Y")
    def validate_date_format(date_str):
        try:
            if isinstance(date_str, datetime):
                # If already a datetime object, no need to check format
                return True
            # Attempt to parse string as datetime in the specified format
            datetime.strptime(str(date_str), "%m/%d/%Y")
            return True
        except ValueError:
            # Date format does not match
            return False
    
    # Apply the date validation, remove rows with invalid 'Build #' dates
    if "EG4-DBDump" in filename:
        data = data[data['Build #'].apply(validate_date_format)]
    
    # Calculate dropped items (for reporting purposes)
    duplicates_dropped = initial_count - len(data)
    empty_cells_dropped = initial_count - duplicates_dropped - len(data)  

    return data, empty_cells_dropped, duplicates_dropped

       
def fetch_all_data_from_collection2(db):
    results = list(db.collection2.find({}, {'_id': 0}))  # Fetch all documents from collection 2 for reference 
    df = pd.DataFrame(results)
    #df = df.drop_duplicates(subset=['Test Case', 'Expected Result', 'Actual Result'])   #I tried implementing this here, but ran with sm errors just decided to do right away when exporting to db (ez)
    return df

def fetch_all_data_from_collection1(db):
    results = list(db.collection1.find({}, {'_id': 0}))  # Fetch all documents from collection 1 for reference
    df = pd.DataFrame(results)
    df = df.drop_duplicates() #just to make sure not data in traspassed again even if the collection has been cleaned for every single data for --insert gathering the data again should also affect if any duplicate traspassed the entry
    return df

def fetch_specific_test_cases_as_df(db, collection_name, include_first=False, include_middle=False, include_last=False):
    collection = db[collection_name]
    test_cases = list(collection.find({}, {'_id': 0}).sort("Test #", 1))
    
    if not test_cases:
        return pd.DataFrame()  # Return an empty DataFrame if no test cases are found

    df = pd.DataFrame(test_cases)
    
    requested_rows = pd.DataFrame()  # Ensure this is a DataFrame
    if include_first:
        requested_rows = pd.concat([requested_rows, df.iloc[0:1]])
    if include_middle:
        middle_index = len(df) // 2   #easier by just letting the middle index be determined by the lenght to be a bit more accurate
        requested_rows = pd.concat([requested_rows, df.iloc[middle_index:middle_index+1]])
    if include_last:
        requested_rows = pd.concat([requested_rows, df.iloc[-1:]])
    
    return requested_rows.reset_index(drop=True)




def find_repeatable_bugs(db):
    query = {"Repeatable?": {"$regex": "^yes$", "$options": "i"}}
    results1 = list(db.collection1.find(query, {'_id': 0}))
    results2 = list(db.collection2.find(query, {'_id': 0}))
    
    # Combine and remove duplicates. 
    combined_df = pd.concat([pd.DataFrame(results1), pd.DataFrame(results2)], ignore_index=True).drop_duplicates()
    return combined_df


def find_blocker_bugs(db):
    # Use a regex match for an exact case-insensitive "yes"
    query = {"Blocker?": {"$regex": "^yes$", "$options": "i"}}
    results1 = list(db.collection1.find(query, {'_id': 0}))
    results2 = list(db.collection2.find(query, {'_id': 0}))
    combined_df = pd.concat([pd.DataFrame(results1), pd.DataFrame(results2)], ignore_index=True).drop_duplicates()
    return combined_df

def reports_on_build(db, build_date_str):
    try:
        # Convert the input date string to datetime object
        input_date = datetime.strptime(build_date_str, "%m/%d/%Y")
        # Prepare the query date string in the format stored in the database
        query_date_str = input_date.strftime("%Y-%m-%d")
    except ValueError:
        print("Date must be in MM/DD/YYYY format.")
        return pd.DataFrame()  # Return an empty DataFrame if the date format is wrong (just to match the requirement in the assignment) the message is to inform the user the date has to be completed in the right format

    # Construct the query considering the date format in the database (date format in db is saved different than when reading directly from xlxs file)
    query = {"Build #": {"$gte": datetime.strptime(query_date_str, "%Y-%m-%d"), "$lt": datetime.strptime(query_date_str, "%Y-%m-%d") + timedelta(days=1)}}
    
    # Fetch records from both collections
    results1 = list(db.collection1.find(query, {'_id': 0}))
    results2 = list(db.collection2.find(query, {'_id': 0}))
    
    # Combine the results from both collections, ensuring no duplicates
    combined_results = results1 + results2
    df_combined = pd.DataFrame(combined_results).drop_duplicates()

    if not df_combined.empty:
        return df_combined
    else:
        print(f"No reports found for the build date {build_date_str}.")
        return pd.DataFrame()


def find_reports_by_user(db, user_name, collection_name):
    # Construct a case-insensitive query for the user name
    query = {"Test Owner": {"$regex": f"^{user_name}$", "$options": "i"}}
    results = list(db[collection_name].find(query, {'_id': 0}))
    df = pd.DataFrame(results)
    return df

#export logic for the csv files in order to save the data if user want to (the code itself already includes the ability to show other outputs if desired)

def export_to_csv(df, filename):
    if not df.empty:
        # Check if 'Build #' column exists and is of datetime type
        if 'Build #' in df.columns and pd.api.types.is_datetime64_any_dtype(df['Build #']):
            df['Build #'] = df['Build #'].dt.strftime('%m/%d/%Y')  # Format date
        df.to_csv(filename, index=False)
        print(f"Data exported to {filename}.")
    else:
        print("No data to export.")





def insert_data(collection, data):
    collection.insert_many(data.to_dict('records'))

def main():
    args = parse_arguments()
    db = connect_to_database()
    collection_name = None

    if args.insert:
        print(f"Inserting data from {args.insert} into the database...")
        data, empty_cells_dropped, duplicates_dropped = read_and_process_file(args.insert)
        # Choose the collection based on the filename
        collection = db.collection1 if "EG4-DBDump" not in args.insert else db.collection2
        # Insert the data into the chosen collection
        insert_data(collection, data)
        cleaned_reports = len(data)

        # Print success message with details about the insertion
        print(f"Data inserted successfully into {'collection1' if 'EG4-DBDump' not in args.insert else 'collection2'}. {empty_cells_dropped} items dropped due to empty cells, {duplicates_dropped} duplicate item dropped. Number of cleaned reports: {cleaned_reports}.")
        display_data_with_pandastable(data)
        #print(df)
        


    if args.repeatables:
        # Fetch data, df divided for better readability and more functionality within the data and the program
        df = find_repeatable_bugs(db)
        if not df.empty:
            if args.export_csv:
            # Export the DataFrame to a CSV file
                export_to_csv(df, "repeatable_bugs.csv")
                #print(df) (uncomment if need to see the data for repeatables)
            else:
                # If not exporting, display the data (used for the other dependencies)
                display_data_with_pandastable(df)
                print("\nRepeatable Bugs Details:")
                print(df)

        else:
            print("No repeatable bugs found.")
        

    if args.blocker:
        # Fetch and display blocker bugs from both collections
        df = find_blocker_bugs(db)
        if not df.empty:
            if args.export_csv:
                export_to_csv(df, "repeatable_blockers.csv")
                #print(df) (uncomment if need to see the data for blocker)
            else:
            # If not exporting, display the data 
                display_data_with_pandastable(df)
                print("\nRepeatable Blockers Details:")
                print(df)

        else:
            print("No repeatable blockers found.")

    if args.date:
        df = reports_on_build(db, args.date)
        if not df.empty:
            if args.export_csv:
                export_to_csv(df, "date_selected.csv")
                print(df)
            else:
                display_data_with_pandastable(df)
                print("\nReports on Build Desired:")
                print(df)
        else:
            print(f"No reports found for the build date {args.date}.")

    if args.weekly:
        collection_name = "collection1"
        df = fetch_all_data_from_collection1(db)
        if not df.empty:
            if args.export_csv:
                export_to_csv(df, "weekly_collection1_data.csv")
            else:
                display_data_with_pandastable(df)
                print("\nWeekly Data")
                print(df)
        else:
            print("No data found in Weekly collection")

    if args.mega:
        # use argument to fetch the data from there to any other argument in the parsing 
        df = fetch_all_data_from_collection2(db)
        collection_name = "collection2"
        if not df.empty:
            if args.export_csv:
                export_to_csv(df, "mega_collection_data.csv")
            else:
                display_data_with_pandastable(df)
                print("\nData in Mega Collection")
                #print(df)
        else:
            print("Collection 2 (Mega) is empty.")

    if args.user:
        
        if not collection_name:
            print("Please specify --weekly or --mega with --user ")
            #this will set only to search user in specific data 
           
        else:
            df = find_reports_by_user(db, args.user, collection_name)
            if not df.empty:
                if args.export_csv:
                    
                    sanitized_user_name = args.user.replace(" ", "_").replace("/", "_").replace("\\", "_")
                    csv_filename = f"{sanitized_user_name}_{collection_name}_reports.csv"
                    export_to_csv(df, csv_filename)
                else:
                    
                    display_data_with_pandastable(df)
                    print("\nUser Reports Details:")
                    print(df)
            else:
                print(f"No reports found for Test Owner {args.user} in {collection_name}. (thanos his data bruh)")

    

    if collection_name and (args.first or args.middle or args.last):
        df = fetch_specific_test_cases_as_df(db, collection_name, args.first, args.middle, args.last)
        if not df.empty:
            print("Requested Test Cases:")
            print(df)  # Print in terminal using pandas again
            display_data_with_pandastable(df)


            if args.export_csv:
                #this will define the dependencies only when the entry of --first --middle --third is input 
                parts = ["test_cases"]
                if args.first:
                    parts.append("first")
                if args.middle:
                    parts.append("middle")
                if args.last:
                    parts.append("last")
                filename = "_".join(parts) + ".csv"
                export_to_csv(df, filename)
        else:
            print("No test cases found.")
    elif args.first or args.middle or args.last:
        print("No collection specified for --first, --middle, or --last. Please use --weekly or --mega.")



    

    
if __name__ == "__main__":
    main()


