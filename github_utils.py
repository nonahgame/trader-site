# github_utils.py
from flask_setup import GITHUB_API_URL, HEADERS, logger, db_path
import requests
import base64

def upload_to_github(file_path, file_name):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return
        if not GITHUB_REPO or GITHUB_REPO == "GITHUB_REPO":
            logger.error("GITHUB_REPO is not set or invalid.")
            return
        if not GITHUB_PATH:
            logger.error("GITHUB_PATH is not set.")
            return
        logger.debug(f"Uploading {file_name} to GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        with open(file_path, "rb") as f:
            content = base64.b64encode(f.read()).decode("utf-8")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        sha = None
        if response.status_code == 200:
            sha = response.json().get("sha")
            logger.debug(f"Existing file SHA: {sha}")
        elif response.status_code != 404:
            logger.error(f"Failed to check existing file on GitHub: {response.status_code} - {response.text}")
            return
        payload = {
            "message": f"Update {file_name} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            "content": content
        }
        if sha:
            payload["sha"] = sha
        response = requests.put(GITHUB_API_URL, headers=HEADERS, json=payload)
        if response.status_code in [200, 201]:
            logger.info(f"Successfully uploaded {file_name} to GitHub")
        else:
            logger.error(f"Failed to upload {file_name} to GitHub: {response.status_code} - {response.text}")
    except Exception as e:
        logger.error(f"Error uploading {file_name} to GitHub: {e}", exc_info=True)

def download_from_github(file_name, destination_path):
    try:
        if not GITHUB_TOKEN or GITHUB_TOKEN == "GITHUB_TOKEN":
            logger.error("GITHUB_TOKEN is not set or invalid.")
            return False
        if not GITHUB_REPO or GITHUB_REPO == "GITHUB_REPO":
            logger.error("GITHUB_REPO is not set or invalid.")
            return False
        if not GITHUB_PATH:
            logger.error("GITHUB_PATH is not set.")
            return False
        logger.debug(f"Downloading {file_name} from GitHub: {GITHUB_REPO}/{GITHUB_PATH}")
        response = requests.get(GITHUB_API_URL, headers=HEADERS)
        if response.status_code == 404:
            logger.info(f"No {file_name} found in GitHub repository. Starting with a new database.")
            return False
        elif response.status_code != 200:
            logger.error(f"Failed to fetch {file_name} from GitHub: {response.status_code} - {response.text}")
            return False
        content = base64.b64decode(response.json()["content"])
        with open(destination_path, "wb") as f:
            f.write(content)
        logger.info(f"Downloaded {file_name} from GitHub to {destination_path}")
        return True
    except Exception as e:
        logger.error(f"Error downloading {file_name} from GitHub: {e}", exc_info=True)
        return False
