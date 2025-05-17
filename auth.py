import streamlit as st
import hashlib
import sqlite3
from datetime import datetime
from database import Database

def hash_password(password):
    """Create a SHA-256 hash of the password"""
    return hashlib.sha256(password.encode()).hexdigest()

def create_user(username, password, db):
    """Create a new user in the database"""
    hashed_password = hash_password(password)
    try:
        db.execute("INSERT INTO users (username, password, created_at) VALUES (?, ?, ?)",
                  (username, hashed_password, datetime.now()))
        return True
    except sqlite3.IntegrityError:
        return False  # Username already exists

def verify_user(username, password, db):
    """Verify user credentials"""
    hashed_password = hash_password(password)
    result = db.query_one("SELECT * FROM users WHERE username = ? AND password = ?", 
                         (username, hashed_password))
    return result is not None

def authenticate():
    """Handle user authentication"""
    # Initialize database
    db = Database()
    
    # Initialize session state for authentication
    if 'authenticated' not in st.session_state:
        st.session_state.authenticated = False
    
    if 'username' not in st.session_state:
        st.session_state.username = None
    
    # If already authenticated, return authentication status
    if st.session_state.authenticated:
        return True, st.session_state.username
    
    # Create a form for authentication
    st.title("WSCAD Excel Comparison System")
    
    # Create tabs for login and registration
    login_tab, register_tab = st.tabs(["Login", "Register"])
    
    with login_tab:
        with st.form("login_form"):
            st.subheader("Login")
            username = st.text_input("Username")
            password = st.text_input("Password", type="password")
            login_submit = st.form_submit_button("Login")
            
            if login_submit:
                if username and password:
                    if verify_user(username, password, db):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        db.log_activity(f"User logged in: {username}", username)
                        st.success("Login successful!")
                        st.rerun()
                    else:
                        st.error("Invalid username or password")
                else:
                    st.warning("Please enter both username and password")
    
    with register_tab:
        with st.form("register_form"):
            st.subheader("Register")
            new_username = st.text_input("Username")
            new_password = st.text_input("Password", type="password")
            confirm_password = st.text_input("Confirm Password", type="password")
            register_submit = st.form_submit_button("Register")
            
            if register_submit:
                if new_username and new_password and confirm_password:
                    if new_password != confirm_password:
                        st.error("Passwords do not match")
                    else:
                        if create_user(new_username, new_password, db):
                            st.success("Registration successful! You can now log in.")
                            db.log_activity(f"New user registered: {new_username}", new_username)
                        else:
                            st.error("Username already exists")
                else:
                    st.warning("Please fill in all fields")
    
    # Add a demo account option
    with st.expander("Demo Account"):
        st.write("Use the following credentials for demo access:")
        st.code("Username: demo\nPassword: demo123")
        if st.button("Login as Demo User"):
            # Check if demo user exists, create if not
            if not verify_user("demo", "demo123", db):
                create_user("demo", "demo123", db)
            
            # Log in as demo user
            st.session_state.authenticated = True
            st.session_state.username = "demo"
            db.log_activity("Demo user logged in", "demo")
            st.success("Logged in as demo user!")
            st.rerun()
    
    return False, None
