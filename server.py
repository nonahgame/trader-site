#import os
#from googleapiclient.discovery import build
#from googleapiclient.http import MediaFileUpload
#from google_auth_oauthlib.flow import InstalledAppFlow
#from google.auth.transport.requests import Request
#from google.oauth2.credentials import Credentials
#from flask import Flask, jsonify

#app = Flask(__name__)

# Google Drive API setup
#SCOPES = ['https://www.googleapis.com/auth/drive.file']
CREDENTIALS_FILE = 'credentials.json'
TOKEN_FILE = 'token.json'
BACKUP_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID')  # Set in Render environment variables

def authenticate_drive():
    """Authenticate with Google Drive API."""
    creds = None
    if os.path.exists(TOKEN_FILE):
        creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
            creds = flow.run_local_server(port=0)
            with open(TOKEN_FILE, 'w') as token:
                token.write(creds.to_json())
    return build('drive', 'v3', credentials=creds)

def upload_to_drive(file_path, mime_type):
    """Upload a file to Google Drive."""
    drive_service = authenticate_drive()
    file_name = os.path.basename(file_path)
    file_metadata = {
        'name': file_name,
        'parents': [BACKUP_FOLDER_ID] if BACKUP_FOLDER_ID else []
    }
    media = MediaFileUpload(file_path, mimetype=mime_type)
    file = drive_service.files().create(
        body=file_metadata,
        media_body=media,
        fields='id, name'
    ).execute()
    return f"Uploaded {file.get('name')} (ID: {file.get('id')})"

@app.route('/backup', methods=['GET'])
def backup_file():
    """Flask endpoint to trigger backup."""
    file_path = os.environ.get('FILE_PATH', 'training_data.csv')
    mime_type = 'text/csv'  # Adjust based on file type (e.g., 'application/octet-stream' for .pkl)
    try:
        result = upload_to_drive(file_path, mime_type)
        return jsonify({'status': 'success', 'message': result})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)})

@app.route('/')
def home():
    return jsonify({'message': 'Backup API is running'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=int(os.environ.get('PORT', 10000)))

#  app1.py
