import streamlit as st
import pandas as pd
import sqlite3
import datetime
import sqlalchemy
import uuid
from sqlalchemy import create_engine
import hashlib
import os
from dotenv import load_dotenv

# Load environment variables (database connections, etc.)
load_dotenv()

# Status constants
STATUS_PENDING_MANAGER = "Pending Manager Approval"
STATUS_REJECTED_MANAGER = "Rejected by Manager"
STATUS_PENDING_PROD = "Pending Production Support Approval"
STATUS_REJECTED_PROD = "Rejected by Production Support"
STATUS_APPROVED = "Approved"
STATUS_EXECUTED = "Executed"
STATUS_FAILED = "Execution Failed"

# Initialize session state for authentication
if 'authenticated' not in st.session_state:
    st.session_state.authenticated = False
if 'username' not in st.session_state:
    st.session_state.username = None
if 'role' not in st.session_state:
    st.session_state.role = None
if 'current_page' not in st.session_state:
    st.session_state.current_page = "login"

# Database connection for the application itself
def get_app_db_connection():
    conn = sqlite3.connect('dml_tool.db')
    return conn

# Initialize application database
def init_app_db():
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    # Create users table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS users (
        username TEXT PRIMARY KEY,
        password_hash TEXT NOT NULL,
        role TEXT NOT NULL,
        email TEXT NOT NULL
    )
    ''')
    
    # Create requests table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS dml_requests (
        request_id TEXT PRIMARY KEY,
        requestor TEXT NOT NULL,
        dml_statement TEXT NOT NULL,
        target_db TEXT NOT NULL,
        target_schema TEXT NOT NULL,
        status TEXT NOT NULL,
        created_date TIMESTAMP NOT NULL,
        manager_username TEXT,
        manager_comments TEXT,
        manager_action_date TIMESTAMP,
        support_username TEXT,
        support_comments TEXT,
        support_action_date TIMESTAMP,
        execution_date TIMESTAMP,
        execution_result TEXT
    )
    ''')
    
    # Create db_connections table
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS db_connections (
        env_name TEXT PRIMARY KEY,
        connection_string TEXT NOT NULL,
        description TEXT
    )
    ''')
    
    conn.commit()
    conn.close()

# Function to hash passwords
def hash_password(password):
    return hashlib.sha256(password.encode()).hexdigest()

# Function to authenticate users
def authenticate(username, password):
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    cursor.execute(
        "SELECT username, role FROM users WHERE username = ? AND password_hash = ?", 
        (username, hash_password(password))
    )
    
    user = cursor.fetchone()
    conn.close()
    
    if user:
        return True, user[0], user[1]
    return False, None, None

# Function to create a new DML request
def create_dml_request(username, dml_statement, target_db, target_schema):
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    request_id = str(uuid.uuid4())
    created_date = datetime.datetime.now()
    
    cursor.execute(
        """
        INSERT INTO dml_requests 
        (request_id, requestor, dml_statement, target_db, target_schema, status, created_date)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (request_id, username, dml_statement, target_db, target_schema, 
         STATUS_PENDING_MANAGER, created_date)
    )
    
    conn.commit()
    conn.close()
    return request_id

# Function to get pending requests for a manager
def get_pending_manager_requests():
    conn = get_app_db_connection()
    df = pd.read_sql_query(
        f"SELECT * FROM dml_requests WHERE status = '{STATUS_PENDING_MANAGER}'",
        conn
    )
    conn.close()
    return df

# Function to get pending requests for production support
def get_pending_prod_requests():
    conn = get_app_db_connection()
    df = pd.read_sql_query(
        f"SELECT * FROM dml_requests WHERE status = '{STATUS_PENDING_PROD}'",
        conn
    )
    conn.close()
    return df

# Function to update request status by manager
def update_manager_decision(request_id, manager_username, approved, comments):
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    action_date = datetime.datetime.now()
    status = STATUS_PENDING_PROD if approved else STATUS_REJECTED_MANAGER
    
    cursor.execute(
        """
        UPDATE dml_requests 
        SET status = ?, manager_username = ?, manager_comments = ?, manager_action_date = ?
        WHERE request_id = ?
        """,
        (status, manager_username, comments, action_date, request_id)
    )
    
    conn.commit()
    conn.close()

# Function to update request status by production support
def update_prod_support_decision(request_id, support_username, approved, comments):
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    action_date = datetime.datetime.now()
    status = STATUS_APPROVED if approved else STATUS_REJECTED_PROD
    
    cursor.execute(
        """
        UPDATE dml_requests 
        SET status = ?, support_username = ?, support_comments = ?, support_action_date = ?
        WHERE request_id = ?
        """,
        (status, support_username, comments, action_date, request_id)
    )
    
    conn.commit()
    conn.close()

# Function to execute approved DML
def execute_dml_request(request_id):
    conn = get_app_db_connection()
    cursor = conn.cursor()
    
    # Get the request details
    cursor.execute("SELECT dml_statement, target_db, target_schema FROM dml_requests WHERE request_id = ?", 
                   (request_id,))
    request = cursor.fetchone()
    
    if not request:
        return False, "Request not found"
    
    dml_statement, target_db, target_schema = request
    
    # Get connection string
    cursor.execute("SELECT connection_string FROM db_connections WHERE env_name = ?", 
                   (target_db,))
    connection_result = cursor.fetchone()
    
    if not connection_result:
        return False, f"No connection configuration found for {target_db}"
    
    connection_string = connection_result[0]
    
    # Execute the DML statement
    try:
        engine = create_engine(connection_string)
        with engine.connect() as db_conn:
            # Set schema if needed
            if target_schema:
                db_conn.execute(f"USE {target_schema}")
            
            # Execute the DML
            db_conn.execute(dml_statement)
            
        execution_date = datetime.datetime.now()
        execution_result = "Success"
        status = STATUS_EXECUTED
    except Exception as e:
        execution_date = datetime.datetime.now()
        execution_result = str(e)
        status = STATUS_FAILED
    
    # Update the request status
    cursor.execute(
        """
        UPDATE dml_requests 
        SET status = ?, execution_date = ?, execution_result = ?
        WHERE request_id = ?
        """,
        (status, execution_date, execution_result, request_id)
    )
    
    conn.commit()
    conn.close()
    
    return status == STATUS_EXECUTED, execution_result

# Function to get user's request history
def get_user_requests(username):
    conn = get_app_db_connection()
    df = pd.read_sql_query(
        "SELECT * FROM dml_requests WHERE requestor = ? ORDER BY created_date DESC",
        conn, params=(username,)
    )
    conn.close()
    return df

# Initialize the database
init_app_db()

# Main application
def main():
    st.title("DML Management Tool")
    
    # Navigation based on authentication and role
    if not st.session_state.authenticated:
        login_page()
    else:
        # Sidebar navigation
        st.sidebar.title(f"Welcome, {st.session_state.username}")
        st.sidebar.write(f"Role: {st.session_state.role}")
        
        # Navigation options based on role
        if st.session_state.role == "requestor":
            page = st.sidebar.radio("Navigation", 
                                   ["New DML Request", "My Requests"])
            
            if page == "New DML Request":
                new_request_page()
            elif page == "My Requests":
                my_requests_page()
                
        elif st.session_state.role == "manager":
            page = st.sidebar.radio("Navigation", 
                                   ["Pending Approvals", "My Decisions"])
            
            if page == "Pending Approvals":
                manager_approval_page()
            elif page == "My Decisions":
                manager_decisions_page()
                
        elif st.session_state.role == "support":
            page = st.sidebar.radio("Navigation", 
                                   ["Pending Approvals", "Approved Requests"])
            
            if page == "Pending Approvals":
                prod_support_approval_page()
            elif page == "Approved Requests":
                execute_approved_page()
        
        # Logout button
        if st.sidebar.button("Logout"):
            st.session_state.authenticated = False
            st.session_state.username = None
            st.session_state.role = None
            st.session_state.current_page = "login"
            st.experimental_rerun()

# Login page
def login_page():
    st.header("Login")
    
    username = st.text_input("Username")
    password = st.text_input("Password", type="password")
    
    if st.button("Login"):
        success, user, role = authenticate(username, password)
        if success:
            st.session_state.authenticated = True
            st.session_state.username = user
            st.session_state.role = role
            st.success("Login successful!")
            st.experimental_rerun()
        else:
            st.error("Invalid username or password")

# New DML request page
def new_request_page():
    st.header("Submit New DML Request")
    
    # Get available databases
    conn = get_app_db_connection()
    dbs = pd.read_sql_query("SELECT env_name, description FROM db_connections", conn)
    conn.close()
    
    db_options = dbs['env_name'].tolist() if not dbs.empty else []
    
    # Form for DML submission
    with st.form("dml_request_form"):
        dml_statement = st.text_area("DML Statement", height=200)
        target_db = st.selectbox("Target Database", db_options)
        target_schema = st.text_input("Target Schema")
        
        submitted = st.form_submit_button("Submit Request")
        
        if submitted:
            if not dml_statement.strip():
                st.error("DML statement cannot be empty")
            elif not target_schema.strip():
                st.error("Target schema cannot be empty")
            else:
                request_id = create_dml_request(
                    st.session_state.username, 
                    dml_statement, 
                    target_db, 
                    target_schema
                )
                st.success(f"Request submitted successfully! Request ID: {request_id}")

# View my requests page
def my_requests_page():
    st.header("My DML Requests")
    
    requests_df = get_user_requests(st.session_state.username)
    
    if requests_df.empty:
        st.info("You haven't submitted any DML requests yet.")
    else:
        # Display requests in a table
        st.dataframe(requests_df[['request_id', 'dml_statement', 'target_db', 
                                  'target_schema', 'status', 'created_date']])
        
        # Allow viewing details of a specific request
        request_ids = requests_df['request_id'].tolist()
        selected_request = st.selectbox("Select a request to view details", request_ids)
        
        if selected_request:
            request_details = requests_df[requests_df['request_id'] == selected_request].iloc[0]
            
            st.subheader("Request Details")
            st.write(f"Status: {request_details['status']}")
            st.write(f"Created: {request_details['created_date']}")
            
            st.subheader("DML Statement")
            st.code(request_details['dml_statement'], language="sql")
            
            # Display approval information if available
            if not pd.isna(request_details['manager_username']):
                st.subheader("Manager Review")
                st.write(f"Manager: {request_details['manager_username']}")
                st.write(f"Date: {request_details['manager_action_date']}")
                st.write(f"Comments: {request_details['manager_comments']}")
            
            if not pd.isna(request_details['support_username']):
                st.subheader("Production Support Review")
                st.write(f"Support: {request_details['support_username']}")
                st.write(f"Date: {request_details['support_action_date']}")
                st.write(f"Comments: {request_details['support_comments']}")
            
            if not pd.isna(request_details['execution_date']):
                st.subheader("Execution Details")
                st.write(f"Date: {request_details['execution_date']}")
                st.write(f"Result: {request_details['execution_result']}")

# Manager approval page
def manager_approval_page():
    st.header("Pending Manager Approvals")
    
    pending_requests = get_pending_manager_requests()
    
    if pending_requests.empty:
        st.info("No requests pending approval.")
    else:
        # Display pending requests
        st.dataframe(pending_requests[['request_id', 'requestor', 'dml_statement', 
                                     'target_db', 'target_schema', 'created_date']])
        
        # Select a request to review
        request_ids = pending_requests['request_id'].tolist()
        selected_request = st.selectbox("Select a request to review", request_ids)
        
        if selected_request:
            request_details = pending_requests[pending_requests['request_id'] == selected_request].iloc[0]
            
            st.subheader("Request Details")
            st.write(f"Requestor: {request_details['requestor']}")
            st.write(f"Created: {request_details['created_date']}")
            
            st.subheader("DML Statement")
            st.code(request_details['dml_statement'], language="sql")
            
            # Approval form
            with st.form("manager_approval_form"):
                decision = st.radio("Decision", ["Approve", "Reject"])
                comments = st.text_area("Comments")
                
                submitted = st.form_submit_button("Submit Decision")
                
                if submitted:
                    approved = decision == "Approve"
                    update_manager_decision(
                        selected_request, 
                        st.session_state.username, 
                        approved, 
                        comments
                    )
                    
                    status = "approved" if approved else "rejected"
                    st.success(f"Request {status} successfully!")
                    st.experimental_rerun()

# Manager decisions page
def manager_decisions_page():
    st.header("My Manager Decisions")
    
    conn = get_app_db_connection()
    decisions_df = pd.read_sql_query(
        "SELECT * FROM dml_requests WHERE manager_username = ? ORDER BY manager_action_date DESC",
        conn, params=(st.session_state.username,)
    )
    conn.close()
    
    if decisions_df.empty:
        st.info("You haven't made any decisions yet.")
    else:
        st.dataframe(decisions_df[['request_id', 'requestor', 'status', 
                                 'manager_action_date', 'manager_comments']])

# Production support approval page
def prod_support_approval_page():
    st.header("Pending Production Support Approvals")
    
    pending_requests = get_pending_prod_requests()
    
    if pending_requests.empty:
        st.info("No requests pending approval.")
    else:
        # Display pending requests
        st.dataframe(pending_requests[['request_id', 'requestor', 'dml_statement', 
                                     'target_db', 'target_schema', 'created_date', 
                                     'manager_username']])
        
        # Select a request to review
        request_ids = pending_requests['request_id'].tolist()
        selected_request = st.selectbox("Select a request to review", request_ids)
        
        if selected_request:
            request_details = pending_requests[pending_requests['request_id'] == selected_request].iloc[0]
            
            st.subheader("Request Details")
            st.write(f"Requestor: {request_details['requestor']}")
            st.write(f"Created: {request_details['created_date']}")
            st.write(f"Manager Approved: {request_details['manager_username']}")
            st.write(f"Manager Comments: {request_details['manager_comments']}")
            
            st.subheader("DML Statement")
            st.code(request_details['dml_statement'], language="sql")
            
            # Approval form
            with st.form("support_approval_form"):
                decision = st.radio("Decision", ["Approve", "Reject"])
                comments = st.text_area("Comments")
                
                submitted = st.form_submit_button("Submit Decision")
                
                if submitted:
                    approved = decision == "Approve"
                    update_prod_support_decision(
                        selected_request, 
                        st.session_state.username, 
                        approved, 
                        comments
                    )
                    
                    status = "approved" if approved else "rejected"
                    st.success(f"Request {status} successfully!")
                    st.experimental_rerun()

# Execute approved requests page
def execute_approved_page():
    st.header("Execute Approved Requests")
    
    conn = get_app_db_connection()
    approved_df = pd.read_sql_query(
        f"SELECT * FROM dml_requests WHERE status = '{STATUS_APPROVED}'",
        conn
    )
    conn.close()
    
    if approved_df.empty:
        st.info("No approved requests pending execution.")
    else:
        # Display approved requests
        st.dataframe(approved_df[['request_id', 'requestor', 'dml_statement', 
                                'target_db', 'target_schema', 'created_date']])
        
        # Select a request to execute
        request_ids = approved_df['request_id'].tolist()
        selected_request = st.selectbox("Select a request to execute", request_ids)
        
        if selected_request:
            request_details = approved_df[approved_df['request_id'] == selected_request].iloc[0]
            
            st.subheader("Request Details")
            st.write(f"Requestor: {request_details['requestor']}")
            st.write(f"Target: {request_details['target_db']}.{request_details['target_schema']}")
            
            st.subheader("DML Statement")
            st.code(request_details['dml_statement'], language="sql")
            
            # Execute button
            if st.button("Execute DML"):
                success, result = execute_dml_request(selected_request)
                
                if success:
                    st.success("DML executed successfully!")
                else:
                    st.error(f"Execution failed: {result}")
                
                st.experimental_rerun()

if __name__ == "__main__":
    main()