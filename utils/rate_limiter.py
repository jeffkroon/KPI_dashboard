import time
import threading
from functools import wraps
from collections import defaultdict

# Thread-safe rate limiter per user (by username or IP)
class RateLimiter:
    def __init__(self, max_calls: int, period: int):
        self.max_calls = max_calls
        self.period = period  # in seconds
        self.calls = defaultdict(list)
        self.lock = threading.Lock()

    def is_allowed(self, user_id: str) -> bool:
        now = time.time()
        with self.lock:
            # Verwijder oude calls
            self.calls[user_id] = [t for t in self.calls[user_id] if now - t < self.period]
            if len(self.calls[user_id]) < self.max_calls:
                self.calls[user_id].append(now)
                return True
            return False

    def rate_limit(self, user_id_func):
        """
        Decorator voor rate limiting per gebruiker.
        user_id_func: functie die de user_id (username, email, of IP) uit de Streamlit session haalt
        """
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                user_id = user_id_func()
                if not self.is_allowed(user_id):
                    raise Exception(f"Rate limit bereikt: max {self.max_calls} acties per {self.period} seconden per gebruiker.")
                return func(*args, **kwargs)
            return wrapper
        return decorator

# Voorbeeld user_id-functie voor Streamlit-authenticator:
def get_user_id():
    import streamlit as st
    # Pas aan naar jouw authenticatie-methode
    return st.session_state.get('username', 'anonymous')

# Instantieer een limiter: max 5 AI-calls per minuut per gebruiker
ai_rate_limiter = RateLimiter(max_calls=5, period=60)

# Gebruik als decorator:
# @ai_rate_limiter.rate_limit(get_user_id)
# def jouw_functie(...):
#     ... 