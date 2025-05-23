import os
import mimetypes
from datetime import datetime
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from flask import Flask, jsonify
import glob

app = Flask(__name__)

# Google Drive API configuration
SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = '/data/credentials.json'  # Stored on Render's persistent disk
TOKEN_FILE = '/data/token.json'  # Stored on Render's persistent disk
BACKUP_DIR = os.environ.get('BACKUP_DIR', '/data/backups')  # Directory for files to back up
DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')  # Parent folder in Google Drive

def authenticate_drive():
    """Authenticate with Google Drive API."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(f"{CREDENTIALS_FILE} not found on persistent disk")
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)  # Local testing only; see deployment notes
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def create_folder(drive_service, folder_name):
    """Create a folder in Google Drive and return its ID."""
    file_metadata = {
        'name': folder_name,
        'mimeType': 'application/vnd.google-apps.folder',
        'parents': [DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []
    }
    folder = drive_service.files().create(body=file_metadata, fields='id').execute()
    return folder.get('id')

def upload_file(drive_service, file_path, folder_id=None):
    """Upload a single file to Google Drive."""
    file_name = os.path.basename(file_path)
    mime_type, _ = mimetypes.guess_type(file_path)
    if not mime_type:
        mime_type = 'application/octet-stream'  # Default for unknown types
    file_metadata = {
        'name': file_name,
        'parents': [folder_id] if folder_id else [DRIVE_FOLDER_ID] if DRIVE_FOLDER_ID else []
    }
    media = MediaFileUpload(file_path, mimetype=mime_type)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, name'
    ).execute()
    return f"Uploaded {file.get('name')} (ID: {file.get('id')})"

def delete_file(drive_service, file_id):
    """Delete a file from Google Drive."""
    try:
        drive_service.files().delete(fileId=file_id).execute()
        return f"Deleted file with ID: {file_id}"
    except Exception as e:
        return f"Error deleting file: {e}"

@app.route('/backup', methods=['GET'])
def backup_files():
    """Flask endpoint to back up all files in BACKUP_DIR to a timestamped folder."""
    try:
        drive_service = authenticate_drive()
        # Create a timestamped folder for this backup session
        timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
        folder_id = create_folder(drive_service, f"Backup_{timestamp}")
        
        # Upload all files in the backup directory
        results = []
        for file_path in glob.glob(f"{BACKUP_DIR}/*"):
            if os.path.isfile(file_path):
                result = upload_file(drive_service, file_path, folder_id)
                results.append(result)
        
        if not results:
            return jsonify({'status': 'error', 'message': 'No files found in backup directory'})
        return jsonify({'status': 'success', 'message': results})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/delete/<file_id>', methods=['GET'])
def delete_file_endpoint(file_id):
    """Flask endpoint to delete a specific file by ID."""
    try:
        drive_service = authenticate_drive()
        result = delete_file(drive_service, file_id)
        return jsonify({'status': 'success', 'message': result})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/')
def home():
    """Home endpoint to confirm service is running."""
    return jsonify({'message': 'Google Drive Backup API is running'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))

#  app.py
