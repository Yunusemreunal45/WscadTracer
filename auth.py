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
    st.title("WSCAD Bom Karşılaştırma Sistemi")
    
    # Create tabs for login and registration
    login_tab, register_tab = st.tabs(["Giriş"," Kayıt Ol"])

    with login_tab:
        with st.form("login_form"):
            st.subheader("Giriş yap")
            username = st.text_input("Kullanıcı Adı")
            password = st.text_input("Şifre", type="password")
            login_submit = st.form_submit_button("Giriş Yap")
            
            if login_submit:
                if username and password:
                    if verify_user(username, password, db):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        db.log_activity(f"User logged in: {username}", username)
                        st.success("Giriş başarılı!")
                        st.rerun()
                    else:
                        st.error("Geçersiz kullanıcı adı veya şifre")
                else:
                    st.warning("Lütfen hem kullanıcı adınızı hem de şifrenizi girin")

    with register_tab:
        with st.form("register_form"):
            st.subheader("Kayıt ol")
            new_username = st.text_input("Kullanıcı Adı")
            new_password = st.text_input("Şifre", type="password")
            confirm_password = st.text_input("Şifreyi onayla ", type="password")
            register_submit = st.form_submit_button("Kayıt Ol")
            
            if register_submit:
                if new_username and new_password and confirm_password:
                    if new_password != confirm_password:
                        st.error("Şifreler eşleşmiyor")
                    else:
                        if create_user(new_username, new_password, db):
                            st.success("Registration successful! You can now log in.")
                            db.log_activity(f"New user registered: {new_username}", new_username)
                        else:
                            st.error("Kullanıcı adı zaten mevcut")
                else:
                    st.warning("Lütfen tüm alanları doldurun")
    
    # Add a demo account option
    with st.expander("Demo Hesabı"):
        st.write("Demo erişimi için aşağıdaki kimlik bilgilerini kullanın:")
        st.code("Kullanıcı Adı: demo\n Şifre: demo123")
        if st.button("Demo Kullanıcısı olarak giriş yapın"):
            # Check if demo user exists, create if not
            if not verify_user("demo", "demo123", db):
                create_user("demo", "demo123", db)
            
            # Log in as demo user
            st.session_state.authenticated = True
            st.session_state.username = "demo"
            db.log_activity("Demo user logged in", "demo")
            st.success("Demo kullanıcısı olarak giriş yapıldı!")
            st.rerun()
    
    return False, None
