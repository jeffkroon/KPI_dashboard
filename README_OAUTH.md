# OAuth Setup voor Dunion KPI Dashboard

## 🔐 OAuth Authenticatie

Dit dashboard gebruikt Google OAuth voor authenticatie via Streamlit's built-in OAuth support.

### Setup Instructies

1. **Google Cloud Console Setup:**
   - Maak een OAuth 2.0 Client ID aan
   - Voeg redirect URI toe: `https://kpi-dashboard-hc4e.onrender.com/oauth2callback`

2. **Environment Variables (Productie):**
   ```bash
   STREAMLIT_AUTH_REDIRECT_URI=https://kpi-dashboard-hc4e.onrender.com/oauth2callback
   STREAMLIT_AUTH_COOKIE_SECRET=<veilige_random_string>
   STREAMLIT_AUTH_CLIENT_ID=<google_client_id>
   STREAMLIT_AUTH_CLIENT_SECRET=<google_client_secret>
   STREAMLIT_AUTH_SERVER_METADATA_URL=https://accounts.google.com/.well-known/openid-configuration
   ```

3. **Lokale Development:**
   - Kopieer `.streamlit/secrets.toml.example` naar `.streamlit/secrets.toml`
   - Vul je eigen credentials in

### Bestanden die NIET gepusht worden:
- `.streamlit/secrets.toml` (bevat echte credentials)
- `.streamlit/secrets_production.toml`
- `deploy_production.sh`

### Toegangscontrole
Alleen e-mails in `utils/allowed_emails.py` hebben toegang tot het dashboard.
