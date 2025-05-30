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
        user_id = db.execute("INSERT INTO users (username, password_hash, created_at) VALUES (?, ?, ?)",
                  (username, hashed_password, datetime.now()))
        return user_id is not None
    except sqlite3.IntegrityError:
        return False  # Username already exists
    except Exception as e:
        print(f"User creation error: {e}")
        return False

def verify_user(username, password, db):
    """Verify user credentials"""
    hashed_password = hash_password(password)
    result = db.query_one("SELECT * FROM users WHERE username = ? AND password_hash = ?", 
                         (username, hashed_password))
    
    if result:
        # Update last login
        db.execute("UPDATE users SET last_login = ? WHERE username = ?", 
                  (datetime.now(), username))
        return True
    return False

def authenticate():
    """Handle user authentication with improved error handling"""
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
    st.title("🔧 WSCAD BOM Karşılaştırma Sistemi")
    st.markdown("### WSCAD Excel dosyalarınızı karşılaştırın ve proje revizyonlarını yönetin")
    
    # Create tabs for login and registration
    login_tab, register_tab = st.tabs(["🔑 Giriş Yap", "📝 Kayıt Ol"])

    with login_tab:
        with st.form("login_form", clear_on_submit=True):
            st.subheader("Sisteme Giriş Yapın")
            username = st.text_input("👤 Kullanıcı Adı", placeholder="kullanici_adi")
            password = st.text_input("🔒 Şifre", type="password", placeholder="şifreniz")
            login_submit = st.form_submit_button("🚀 Giriş Yap", use_container_width=True)
            
            if login_submit:
                if username and password:
                    # Trim whitespace
                    username = username.strip()
                    
                    if verify_user(username, password, db):
                        st.session_state.authenticated = True
                        st.session_state.username = username
                        db.log_activity(f"User logged in: {username}", username)
                        st.success("✅ Giriş başarılı! Sistem yükleniyor...")
                        st.rerun()
                    else:
                        st.error("❌ Geçersiz kullanıcı adı veya şifre")
                        st.info("💡 Demo hesabı kullanmak isterseniz aşağıdaki demo bölümünü kullanın")
                else:
                    st.warning("⚠️ Lütfen hem kullanıcı adınızı hem de şifrenizi girin")

    with register_tab:
        with st.form("register_form", clear_on_submit=True):
            st.subheader("Yeni Hesap Oluşturun")
            new_username = st.text_input("👤 Kullanıcı Adı", placeholder="en az 3 karakter")
            new_password = st.text_input("🔒 Şifre", type="password", placeholder="en az 6 karakter")
            confirm_password = st.text_input("🔒 Şifreyi Onayla", type="password", placeholder="şifrenizi tekrar girin")
            full_name = st.text_input("👨‍💼 Ad Soyad (İsteğe bağlı)", placeholder="Ad Soyad")
            register_submit = st.form_submit_button("📝 Kayıt Ol", use_container_width=True)
            
            if register_submit:
                if new_username and new_password and confirm_password:
                    # Trim whitespace
                    new_username = new_username.strip()
                    full_name = full_name.strip() if full_name else ""
                    
                    # Validation
                    if len(new_username) < 3:
                        st.error("❌ Kullanıcı adı en az 3 karakter olmalı")
                    elif len(new_password) < 6:
                        st.error("❌ Şifre en az 6 karakter olmalı")
                    elif new_password != confirm_password:
                        st.error("❌ Şifreler eşleşmiyor")
                    else:
                        # Create user with additional info
                        if create_user_with_info(new_username, new_password, full_name, db):
                            st.success("✅ Kayıt başarılı! Artık giriş yapabilirsiniz.")
                            db.log_activity(f"New user registered: {new_username}", new_username)
                            st.info("👈 Giriş sekmesinden sisteme giriş yapın")
                        else:
                            st.error("❌ Kullanıcı adı zaten mevcut veya kayıt hatası")
                else:
                    st.warning("⚠️ Lütfen zorunlu alanları doldurun")
    
    # Demo account section - improved
    with st.expander("🎯 Demo Hesabı ile Hızlı Giriş"):
        col1, col2 = st.columns([2, 1])
        
        with col1:
            st.write("**Demo hesabı ile sistemi hemen test edebilirsiniz:**")
            st.code("👤 Kullanıcı Adı: demo\n🔒 Şifre: demo123")
            st.caption("Demo hesabı ile tüm özellikleri deneyebilirsiniz")
        
        with col2:
            if st.button("🚀 Demo Girişi", use_container_width=True):
                # Create demo user if not exists
                if not verify_user("demo", "demo123", db):
                    create_user_with_info("demo", "demo123", "Demo Kullanıcı", db)
                
                # Log in as demo user
                st.session_state.authenticated = True
                st.session_state.username = "demo"
                db.log_activity("Demo user logged in", "demo")
                st.success("✅ Demo kullanıcısı olarak giriş yapıldı!")
                st.rerun()
    
    # System info
    with st.expander("ℹ️ Sistem Hakkında"):
        st.markdown("""
        **WSCAD BOM Karşılaştırma Sistemi Özellikleri:**
        - 📊 WSCAD Excel dosyalarını otomatik tanıma
        - 🔄 BOM karşılaştırma ve değişiklik analizi  
        - 📈 Proje revizyonu takibi
        - ☁️ Supabase ile bulut senkronizasyonu
        - 📥 ERP sistemlerine veri aktarımı
        - 📋 Detaylı raporlama
        """)
    
    return False, None

def create_user_with_info(username, password, full_name, db):
    """Create user with additional information"""
    hashed_password = hash_password(password)
    try:
        user_id = db.execute("""
            INSERT INTO users (username, password_hash, full_name, created_at, is_active) 
            VALUES (?, ?, ?, ?, ?)
        """, (username, hashed_password, full_name, datetime.now(), True))
        return user_id is not None
    except sqlite3.IntegrityError:
        return False  # Username already exists
    except Exception as e:
        print(f"User creation with info error: {e}")
        return False

def get_user_info(username, db):
    """Get user information"""
    return db.query_one("""
        SELECT username, full_name, created_at, last_login 
        FROM users WHERE username = ?
    """, (username,))

def update_user_last_login(username, db):
    """Update user's last login time"""
    try:
        db.execute("UPDATE users SET last_login = ? WHERE username = ?", 
                  (datetime.now(), username))
        return True
    except Exception as e:
        print(f"Update last login error: {e}")
        return False