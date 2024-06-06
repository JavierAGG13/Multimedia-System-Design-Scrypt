import argparse

import csv

from pymongo import MongoClient

# Function to parse and input QA CSV data into MongoDB

def parse_and_input_qa_csv(file_path, collection_name, db):

    collection = db[collection_name]

    with open(file_path, 'r') as csvfile:

        csv_reader = csv.DictReader(csvfile)

        for row in csv_reader:

            collection.insert_one(row)

# Function to perform database queries

def perform_database_queries(db):

    # Query 1: List all work done by Your user - from both collections (No duplicates)

    work_done_by_user = db.collection1.find({'User': 'YourUser'}, {'Work Done': 1}).distinct('Work Done')

    print("Work done by YourUser:")

    for work in work_done_by_user:

        print(work)

    # Query 2: All repeatable bugs - from both collections (No duplicates)

    repeatable_bugs = db.collection1.find({'Bug Type': 'Repeatable'}, {'Bug Description': 1}).distinct('Bug Description')

    print("\nRepeatable bugs:")

    for bug in repeatable_bugs:

        print(bug)

    # Query 3: All Blocker bugs - from both collections (No duplicates)

    blocker_bugs = db.collection1.find({'Bug Severity': 'Blocker'}, {'Bug Description': 1}).distinct('Bug Description')

    print("\nBlocker bugs:")

    for bug in blocker_bugs:

        print(bug)

    # Query 4: All reports on build 3/19/2024 - from both collections (No duplicates)

    build_reports = db.collection1.find({'Build Date': '3/19/2024'}, {'Report ID': 1}).distinct('Report ID')

    build_reports += db.collection2.find({'Build Date': '3/19/2024'}, {'Report ID': 1}).distinct('Report ID')

    print("\nReports on build 3/19/2024:")

    for report_id in build_reports:

        print(report_id)

    # Query 5: Report back the first, middle, and last test cases from collection 2

    test_cases = list(db.collection2.find({}, {'Test Case': 1}))

    first_test_case = test_cases[0]['Test Case']

    middle_test_case = test_cases[len(test_cases)//2]['Test Case']

    last_test_case = test_cases[-1]['Test Case']

    print("\nFirst test case:", first_test_case)

    print("Middle test case:", middle_test_case)

    print("Last test case:", last_test_case)

# Main function to execute the script

def main():

    # Parse command-line arguments

    parser = argparse.ArgumentParser(description="Parse QA CSV data into MongoDB and perform database queries")

    parser.add_argument("qa_csv_file", help="Path to the QA CSV file")

    parser.add_argument("db_dump_file", help="Path to the DB dump file")

    args = parser.parse_args()

    # Connect to MongoDB

    client = MongoClient('mongodb://localhost:27017/')

    db = client['qa_database']

    # Parse and input QA CSV data into Collection 1

    parse_and_input_qa_csv(args.qa_csv_file, 'collection1', db)

    # Parse and input DB dump data into Collection 2

    parse_and_input_qa_csv(args.db_dump_file, 'collection2', db)

    # Perform database queries

    perform_database_queries(db)

if name == "__main__":

    main()