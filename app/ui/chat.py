import streamlit as st
import requests
import time
import os

API_BASE = "http://localhost:8000"  # Change if your FastAPI backend runs elsewhere

st.set_page_config(page_title="🍽️ Restaurant Recommender Chatbot", page_icon="🍽️", layout="wide")

# Add a banner image at the top with reduced height
st.image("static/street-food-still-life.jpg", width=1200, use_container_width=False, caption=None, output_format="auto", channels="RGB", clamp=False)

# --- Session State ---
if "chat_history" not in st.session_state:
    st.session_state.chat_history = []  # List of (role, message)
if "pending_requests" not in st.session_state:
    st.session_state.pending_requests = []  # List of (request_id, user_message)
if "welcomed" not in st.session_state:
    st.session_state.welcomed = False

# --- Reset Button ---
if st.button("🔄 Reset chat"):
    st.session_state.chat_history = []
    st.session_state.pending_requests = []
    st.rerun()

# --- System Welcome Message (only once, not in chat history) ---
if st.session_state.chat_history == []:
    st.session_state.chat_history.append(("assistant", "Hello! I'm your restaurant recommender agent. Tell me what food or cuisine you're craving, and I'll suggest the best places for you!"))

# --- Chat Window with Alignment ---
for role, message in st.session_state.chat_history:
    if role == "user":
        cols = st.columns([0.3, 0.7])
        with cols[1]:
            with st.chat_message("user"):
                st.write(message)
    else:
        cols = st.columns([0.7, 0.3])
        with cols[0]:
            with st.chat_message("assistant"):
                st.write(message)

# --- Input Box ---
user_input = st.chat_input("What food or cuisine are you interested in?")

# --- Send Message Logic ---
if user_input and user_input.strip():
    # Show the user message immediately
    st.session_state.chat_history.append(("user", user_input.strip()))
    try:
        resp = requests.post(f"{API_BASE}/chat/send", json={"desired_food_items": user_input.strip()})
        resp.raise_for_status()
        data = resp.json()
        request_id = data["request_id"]
        st.session_state.pending_requests.append((request_id, user_input.strip()))
        st.rerun()
    except Exception as e:
        st.error(f"Failed to send message: {e}")

# --- Persistent Spinner for Bot Responses ---
if st.session_state.pending_requests:
    spinner_placeholder = st.empty()
    with st.spinner("🤖 Thinking of the best recommendations for you..."):
        new_pending = []
        response_received = False
        for request_id, user_message in st.session_state.pending_requests:
            try:
                resp = requests.get(f"{API_BASE}/chat/message/{request_id}")
                if resp.status_code == 200:
                    data = resp.json()
                    bot_message = data
                    st.session_state.chat_history.append(("assistant", bot_message))
                    new_pending.clear()
                    response_received = True
                else:
                    new_pending.append((request_id, user_message))
            except Exception:
                new_pending.append((request_id, user_message))
        st.session_state.pending_requests = new_pending
        if not response_received and new_pending:
            time.sleep(10)
            st.rerun()
        elif response_received:
            spinner_placeholder.empty()
            st.rerun() 