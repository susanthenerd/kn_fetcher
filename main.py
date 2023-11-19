import json
import sqlite3
from datetime import datetime
import pandas as pd


def create_tables(conn):
    conn.execute('''
    CREATE TABLE IF NOT EXISTS Users (
        ID INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        DisplayName TEXT
    )
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS Problems (
        ID INTEGER PRIMARY KEY,
        Name TEXT NOT NULL,
        Test_Name TEXT,
        Default_Points INTEGER NOT NULL,
        Visible BOOLEAN,
        Visible_Tests BOOLEAN,
        Time_ms REAL NOT NULL,
        Memory_Limit INTEGER NOT NULL,
        Source_Size INTEGER,
        Source_Credits TEXT,
        Console_Input BOOLEAN,
        Score_Precision INTEGER,
        Published_At TEXT,
        Scoring_Strategy TEXT
    )
    ''')

    conn.execute('''
    CREATE TABLE IF NOT EXISTS Submissions (
        ID INTEGER PRIMARY KEY,
        Created_At TEXT NOT NULL,
        User_ID INTEGER NOT NULL,
        Problem_ID INTEGER NOT NULL,
        Contest_ID INTEGER,
        Score INTEGER NOT NULL,
        Compile_Error BOOLEAN NOT NULL,
        Max_Time_ms REAL NOT NULL,
        Max_Memory_bytes INTEGER NOT NULL,
        Language TEXT,
        Code_Size INTEGER,
        Score_Precision INTEGER,
        Submission_Type TEXT,
        ICPC_Verdict TEXT,
        FOREIGN KEY(User_ID) REFERENCES Users(ID),
        FOREIGN KEY(Problem_ID) REFERENCES Problems(ID)
    )
    ''')

    conn.commit()


def insert_users(conn, users_data):
    with conn:
        conn.executemany('''
            INSERT OR IGNORE INTO Users (ID, Name, DisplayName) VALUES (?, ?, ?)
        ''', users_data)


def process_problems_data(problems):
    processed_data = []
    for p in problems.values():
        processed_data.append((
            int(p['id']),
            p['name'],
            p.get('test_name'),
            int(p['default_points']),
            p.get('visible', False),
            p.get('visible_tests', False),
            round(float(p['time_limit']) * 1000, 2),
            int(p['memory_limit']),
            p.get('source_size', None),
            p['source_credits'],
            p.get('console_input', False),
            int(p['score_precision']),
            pd.to_datetime(p['published_at']).strftime('%Y-%m-%d %H:%M:%S') if p['published_at'] else None,
            p.get('scoring_strategy')
        ))
    return processed_data


def insert_problems(conn, problems_data):
    with conn:
        with conn:
            conn.executemany('''
                INSERT OR IGNORE INTO Problems (
                    ID, Name, Test_Name, Default_Points, Visible, Visible_Tests,
                    Time_ms, Memory_Limit, Source_Size, Source_Credits, Console_Input,
                    Score_Precision, Published_At, Scoring_Strategy
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', problems_data)


def insert_submissions(conn, submissions_data):
    with conn:
        conn.executemany('''
            INSERT OR IGNORE INTO Submissions (
                ID, Created_At, User_ID, Problem_ID, Contest_ID, Score, Compile_Error,
                Max_Time_ms, Max_Memory_bytes, Language, Code_Size, Score_Precision,
                Submission_Type, ICPC_Verdict
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', submissions_data)


def process_users_data(users):
    return [(int(uid), u['name'], u['display_name']) for uid, u in users.items()]


def process_submissions_data(submissions):
    processed_data = []
    for sub in submissions:
        if sub['status'] == 'finished':
            processed_data.append((
                int(sub['id']),
                pd.to_datetime(sub['created_at']).strftime('%Y-%m-%d %H:%M:%S'),
                int(sub['user_id']),
                int(sub['problem_id']),
                sub.get('contest_id'),
                int(sub['score']),
                sub.get('compile_error'),
                round(float(sub['max_time']) * 1000, 2),
                int(sub['max_memory']),
                sub.get('language'),
                sub.get('code_size', 0),
                sub.get('score_precision', 0),
                sub.get('submission_type'),
                sub.get('icpc_verdict'),
            ))
    return processed_data


def save_data_to_sqlite(data, db_name="data.db"):
    try:
        conn = sqlite3.connect(db_name)
        create_tables(conn)

        users_data = process_users_data(data["users"])
        insert_users(conn, users_data)

        problems_data = process_problems_data(data["problems"])
        insert_problems(conn, problems_data)

        submissions_data = process_submissions_data(data["submissions"])
        insert_submissions(conn, submissions_data)
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
    finally:
        conn.close()


def main():
    try:
        with open('data_dump_rcpc.json', 'r', encoding='utf-8') as f:
            all_submissions_data = json.load(f)

        if all_submissions_data:
            save_data_to_sqlite(all_submissions_data, "submissions_data_rcpc.db")
    except FileNotFoundError as e:
        print(f"File not found: {e}")
    except json.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")


if __name__ == "__main__":
    main()
