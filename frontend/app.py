import streamlit as st
import pandas as pd
import io
import docx
import json
import requests
from dotenv import load_dotenv
import os
import google.generativeai as genai

# Define the FastAPI endpoint URL (POST endpoint for sending data)
# This must match the host/port where your api.py server is running.
API_ENDPOINT = "http://localhost:8000/process_ai_data"


# Load environment variables
load_dotenv()

# Configure Gemini API
genai.configure(api_key=os.getenv("GEMINI_API_KEY"))

# Initialize the generative model
model = genai.GenerativeModel('gemini-2.5-flash')


# --- Helper Functions (File Extraction) ---

def extract_text_from_file(uploaded_file):
    """Extracts text from various file types."""
    file_type = uploaded_file.type

    if "text" in file_type or file_type == "application/json":
        return uploaded_file.read().decode("utf-8")
    elif file_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        doc = docx.Document(uploaded_file)
        full_text = []
        for para in doc.paragraphs:
            full_text.append(para.text)
        return "\n".join(full_text)
    elif file_type == "application/vnd.ms-excel" or file_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        df = pd.read_excel(uploaded_file)
        return df.to_string(index=False)
    elif file_type == "text/csv":
        df = pd.read_csv(uploaded_file)
        return df.to_string(index=False)
    else:
        st.warning(f"Unsupported file type: {file_type}. Trying to read as plain text.")
        try:
            return uploaded_file.read().decode("utf-8")
        except Exception:
            return None


def generate_table_data_with_gemini(text_data):
    """
    Calls the Gemini model, relying on prompt engineering for reliable JSON output.
    """
    if not text_data:
        return None

    prompt = f"""
    The following is raw, unstructured data. Your task is to analyze the data and extract all relevant information, structuring it into a list of JSON objects (rows).
    The keys in each object must be the appropriate column headers, and the values must be the cell contents for that row.
    Ensure the output is ONLY the raw JSON array. DO NOT include any text, markdown formatting (like ```json), or explanation before or after the JSON array.

    Raw Data:
    {text_data}
    """
    
    try:
        # Use the most stable call signature (content only argument)
        response = model.generate_content(prompt)
        return response.text 
        
    except Exception as e:
        st.error(f"Error communicating with Gemini API: {e}")
        return None


# --- STREAMLIT UI LOGIC ---

st.set_page_config(layout="wide", page_title="AI Data Pipeline Client")

st.title("📄 AI Data Pipeline Client")
st.markdown("Upload your data, generate structured JSON using Gemini, and send it directly to the FastAPI processing API.")

# Input Components
uploaded_file = st.file_uploader(
    "Upload a file (txt, docx, csv, xlsx, etc.)",
    type=["txt", "csv", "docx", "xlsx", "xls", "json"],
    help="Supported formats: text files, Word documents, CSV, Excel spreadsheets, JSON."
)

user_text_input = st.text_area(
    "Or paste your data directly here:",
    height=200,
    help="If both file and text are provided, the file will take precedence."
)

# Process uploaded data or text input
processed_text = None
if uploaded_file is not None:
    st.info(f"Processing uploaded file: {uploaded_file.name}")
    processed_text = extract_text_from_file(uploaded_file)
elif user_text_input:
    st.info("Processing pasted text input.")
    processed_text = user_text_input

json_output_string = None
if processed_text:
    st.subheader("Data Extracted (Preview):")
    st.code(processed_text[:1000] + "..." if len(processed_text) > 1000 else processed_text, language="text")

    if st.button("Generate JSON and Send to API"):
        # 1. GENERATE JSON FROM GEMINI
        with st.spinner("1. Asking Gemini to structure your data..."):
            json_output_string = generate_table_data_with_gemini(processed_text)

        if json_output_string:
            st.subheader("Generated JSON Data:")
            st.code(json_output_string, language="json")
            
            # 2. SEND TO FASTAPI AND GET FETCH DETAILS
            with st.spinner(f"2. Sending structured JSON to FastAPI at {API_ENDPOINT}..."):
                # The payload must match the Pydantic model DataPayload in api.py
                payload = {"json_data_string": json_output_string}
                
                try:
                    response = requests.post(API_ENDPOINT, json=payload)
                    
                    if response.status_code == 200:
                        data = response.json()
                        
                        st.success("✅ Data successfully processed and stored by the API.")
                        
                        # --- DISPLAY FETCH DETAILS FOR THE OTHER BOT ---
                        st.markdown("### 🔑 Data Access Details for Your Other AI Bot:")
                        st.code(f"Unique Data ID (Key):\n{data['data_id']}", language="text")
                        st.code(f"Fetch API Endpoint:\n{data['fetch_endpoint']}", language="text")
                        st.markdown(
                            f"**Note:** This key/endpoint serves the CSV data and will expire after 5 minutes for security/cleanup."
                        )
                        st.markdown("---")
                        # --- END DISPLAY ---

                    else:
                        st.error(f"API Error ({response.status_code}): Could not process data.")
                        st.json(response.json())
                
                except requests.exceptions.ConnectionError:
                    st.error("Connection Error: Could not connect to the FastAPI service. **Ensure `api.py` is running** (`uvicorn api:app --reload`).")
                except Exception as e:
                    st.error(f"An unexpected error occurred during API call: {e}")
            
            # Optional display of data as DataFrame for user confirmation
            try:
                # Clean up the JSON string for internal display only
                clean_json_string = json_output_string
                if clean_json_string.strip().startswith('```'):
                    clean_json_string = clean_json_string.strip().split('```')[1]
                    if clean_json_string.strip().lower().startswith('json'):
                        clean_json_string = clean_json_string.strip()[len('json'):].strip()
                
                data = json.loads(clean_json_string)
                df_display = pd.json_normalize(data)
                st.subheader("Data Processed (In-App Preview for Confirmation):")
                st.dataframe(df_display)
            except (json.JSONDecodeError, Exception):
                st.warning(f"Could not render the internal preview. Please rely on the API response status.")


else:
    st.info("Upload a file or paste text to begin.")

st.markdown("---")
st.markdown("Created with ❤️ using Streamlit and Google Gemini API.")
