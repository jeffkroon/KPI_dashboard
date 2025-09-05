import streamlit as st
import os
try:
    from supabase import create_client, Client
except ImportError:
    st.error("supabase-py is niet geïnstalleerd. Installeer met: pip install supabase")
    st.stop()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    st.error("Supabase credentials ontbreken. Zet SUPABASE_URL en SUPABASE_KEY als environment variables.")
    st.stop()

# Maak Supabase client met betere configuratie voor token refresh
supabase: Client = create_client(
    SUPABASE_URL, 
    SUPABASE_KEY,
    options={
        "auto_refresh_token": True,
        "persist_session": True,
        "detect_session_in_url": True
    }
)

def require_login():
    query_params = st.query_params.to_dict()
    if "access_token" in query_params:
        access_token = query_params["access_token"]
        st.session_state["access_token"] = access_token
        st.success("✅ Ingelogd via Supabase OAuth!")
        st.query_params.clear()  # Leeg de query params
        st.rerun()

    if "access_token" in st.session_state:
        try:
            user_resp = supabase.auth.get_user(st.session_state["access_token"])
        except Exception as e:
            st.error(f"Authenticatie fout: {e}")
            st.session_state.clear()
            st.stop()
        if hasattr(user_resp, "user") and user_resp.user and hasattr(user_resp.user, "email"):
            st.session_state["user_email"] = user_resp.user.email
            return True
        else:
            st.error("Kon gebruikersinfo niet ophalen. Probeer opnieuw in te loggen.")
            st.session_state.clear()
            st.stop()
    else:
        st.markdown(
            '[Log in via de loginpagina](https://kpi-dashboard-1-bmk5.onrender.com/login.html)'
        )
        st.stop()

def require_email_whitelist(allowed_emails):
    import streamlit as st
    user_email = st.session_state.get("user_email", "")
    if user_email not in allowed_emails:
        st.error("Je hebt geen toegang tot dit dashboard.")
        st.stop() 