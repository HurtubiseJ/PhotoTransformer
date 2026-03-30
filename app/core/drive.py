import logging
import os.path
import io
import os
from pathlib import Path

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaFileUpload

logger = logging.getLogger("PhotoTransformer")

CHUNK_SIZE = 1024 * 1024 * 8  # 8MB

DRIVE_NAME = "D1RenderDrive"
PHOTO_QUEUED_NAME = "PhotoQueue"
PHOTO_COMPLETE_NAME = "PhotoComplete"

class DriveService:
    def __init__(self):
        self.scopes = ["https://www.googleapis.com/auth/drive"]

        self.drive_id = ""

        self.photo_queued_id = ""
        self.photo_complete_id = ""

        self.service = None
        self.creds = None

        try:
            self.init_local_data_dirs()
            logger.info(f"Initalized local data folders.")
        except Exception as e:
            raise Exception(f"Failed to initalize local data dirs `/data/downloads` & `/data/complete`. Error: {str(e)}")

        # Load service account credentials from JSON key file
        service_account_file = os.getenv("GOOGLE_SERVICE_ACCOUNT_FILE", "service-account-key.json")

        if not os.path.exists(service_account_file):
            raise Exception(
                f"Service account key file not found: {service_account_file}. "
                "Please set GOOGLE_SERVICE_ACCOUNT_FILE in .env or provide service-account-key.json"
            )

        try:
            self.creds = service_account.Credentials.from_service_account_file(
                service_account_file, scopes=self.scopes
            )
            logger.info(f"Loaded service account credentials from {service_account_file}")
        except Exception as e:
            raise Exception(f"Failed to load service account credentials: {str(e)}") from e

        self.service = build("drive", "v3", credentials=self.creds)

        try:
            self.init_folder_ids()
        except Exception as e:
            raise Exception(f"Failed to find shared drive: {str(e)}") from e

        if not self.service:
            raise Exception("Failed to create Google API service")
        
    def init_local_data_dirs(self):
        if not os.path.exists("/data/downloads"):
            os.mkdir("/data/downloads")
        if not os.path.exists("/data/complete"):
            os.mkdir("/data/complete")

    def init_folder_ids(self):
        # Drive id
        self.drive_id = self.find_drive_id(DRIVE_NAME)

        if not self.drive_id:
            raise Exception(f"Could not find shared drive: {DRIVE_NAME}")

        folders = self.list_items_in_folder(self.drive_id)

        logger.debug(f"Folders found in {DRIVE_NAME}: {[f['name'] for f in folders]}")

        for folder in folders:
            if folder['name'] == PHOTO_QUEUED_NAME:
                self.photo_queued_id = folder['id']
            elif folder['name'] == PHOTO_COMPLETE_NAME:
                self.photo_complete_id = folder['id']

        # Validate all required folders exist
        if not self.photo_complete_id:
            raise Exception(f"Could not find '{PHOTO_COMPLETE_NAME}' folder in {DRIVE_NAME}")
        if not self.photo_queued_id:
            raise Exception(f"Could not find '{PHOTO_QUEUED_NAME}' folder in {DRIVE_NAME}")

        # Archived folder is optional (kept for backwards compatibility but not required)
        logger.info(f"Drive ready - Queue: {self.photo_queued_id}, Complete: {self.photo_complete_id}")
        return

    def close(self):
        self.service.close()

    def find_drive_id(self, drive_name: str):
        drives = self.service.drives().list().execute()
        for d in drives.get("drives", []):
            if d["name"] == drive_name:
                return d['id']
        return None

    def find_folder_id(self, folder_name):
        query = (
            f"name='{folder_name}' and "
            "mimeType='application/vnd.google-apps.folder' and "
            "trashed=false"
        )
        results = self.service.files().list(
            q=query,
            corpora="drive",
            driveId=self.drive_id,
            fields="files(id, name)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()

        folders = results.get("files", [])
        return folders[0]["id"] if folders else None

    def create_folder(self, folder_name, parent_folder_id):
        """
        Creates a folder in Google Drive.

        Args:
            folder_name: Name of the folder to create
            parent_folder_id: Parent folder ID (can be drive ID for root-level folders)

        Returns:
            str: Created folder ID
        """
        folder_metadata = {
            "name": folder_name,
            "mimeType": "application/vnd.google-apps.folder",
            "parents": [parent_folder_id]
        }

        created_folder = self.service.files().create(
            body=folder_metadata,
            fields="id",
            supportsAllDrives=True
        ).execute()

        return created_folder['id']

    def download_file(self, file_id, local_path: str):
        request = self.service.files().get_media(fileId=file_id)

        _local_dir_path = Path(local_path).parent
        if not _local_dir_path.exists():
            os.mkdir(_local_dir_path)

        fh = io.FileIO(local_path, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

    def download_folder(self, folder_id, local_path):
        """
        Recursively downloads a folder and all its contents from Google Drive.

        Args:
            folder_id: Google Drive folder ID to download
            local_path: Local directory path where folder contents will be saved
        """
        # Create the local directory if it doesn't exist
        os.makedirs(local_path, exist_ok=True)

        # List all items in the folder
        items = self.list_items_in_folder(folder_id)

        for item in items:
            item_name = item['name']
            item_id = item['id']
            item_mime_type = item['mimeType']
            item_local_path = os.path.join(local_path, item_name)

            if item_mime_type == 'application/vnd.google-apps.folder':
                # Recursively download subfolder
                self.download_folder(item_id, item_local_path)
            else:
                # Download file
                self.download_file(item_id, item_local_path)

    def upload_file(self, local_path, folder_id):
        """
        Uploads a single file to Google Drive.

        Args:
            local_path: Path to the local file to upload
            folder_id: Google Drive folder ID where the file will be uploaded

        Returns:
            File metadata dict with 'id' on success, None on failure
        """
        try:
            if "/" in local_path:
                file_metadata = {
                    "name": local_path.split("/")[-1],
                    "parents": [folder_id]
                }
            else:    
                file_metadata = {
                    "name": local_path.split("\\")[-1],
                    "parents": [folder_id]
                }

            media = MediaFileUpload(local_path, resumable=True)

            result = self.service.files().create(
                body=file_metadata,
                media_body=media,
                fields="id",
                supportsAllDrives=True
            ).execute()

            return result

        except Exception as e:
            logger.error(f"Error uploading file {local_path}: {e}")
            return None

    def upload_folder(self, local_path, parent_folder_id):
        """
        Recursively uploads a local folder and all its contents to Google Drive.

        Args:
            local_path: Local directory path to upload
            parent_folder_id: Google Drive folder ID where this folder will be uploaded

        Returns:
            The folder ID of the created folder in Google Drive, or None on failure
        """
        try:
            if not os.path.exists(local_path):
                logger.error(f"Local path does not exist: {local_path}")
                return None

            folder_name = os.path.basename(local_path)
            folder_metadata = {
                "name": folder_name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_folder_id]
            }

            created_folder = self.service.files().create(
                body=folder_metadata,
                fields="id",
                supportsAllDrives=True
            ).execute()

            created_folder_id = created_folder['id']
            logger.debug(f"Created Drive folder '{folder_name}' ({created_folder_id})")

            for item_name in os.listdir(local_path):
                item_path = os.path.join(local_path, item_name)

                if os.path.isdir(item_path):
                    subfolder_id = self.upload_folder(item_path, created_folder_id)
                    if not subfolder_id:
                        logger.error(f"Failed to upload subfolder: {item_path}")
                        return None
                else:
                    file_result = self.upload_file(item_path, created_folder_id)
                    if not file_result:
                        logger.error(f"Failed to upload file: {item_path}")
                        return None
                    logger.debug(f"Uploaded: {item_name}")

            return created_folder_id

        except Exception as e:
            logger.error(f"Error in upload_folder for {local_path}: {e}", exc_info=True)
            return None

    def list_items_in_folder(self, folder_id):
        """
        Lists all top level items (folder & files) in a folder.
        """

        query = f"'{folder_id}' in parents and trashed=false"
        results = self.service.files().list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime)", 
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        return results.get("files", [])
    
    def list_files_in_shared_drive_root(self):
        results = self.service.files().list(
            corpora="drive",
            driveId=self.drive_id,
            q="trashed=false",
            fields="files(id,name,mimeType)",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True
        ).execute()
        return results.get("files", [])
    
    def upload_rendered_files(self, local_folder_path, target_folder_id):
        """
        Uploads all files from local rendered output folder directly into a Google Drive folder.

        Args:
            local_folder_path: Local path to folder containing rendered files (e.g., mp4s)
            target_folder_id: Google Drive folder ID where files will be uploaded

        Returns:
            bool: True if all files uploaded successfully, False otherwise
        """
        try:
            if not os.path.exists(local_folder_path):
                logger.error(f"Local path does not exist: {local_folder_path}")
                return False

            uploaded_count = 0
            for item_name in os.listdir(local_folder_path):
                item_path = os.path.join(local_folder_path, item_name)

                if os.path.isfile(item_path):
                    logger.info(f"Uploading {item_name}")
                    file_result = self.upload_file(item_path, target_folder_id)
                    if not file_result:
                        logger.error(f"Failed to upload file: {item_name}")
                        return False
                    uploaded_count += 1
                    logger.info(f"Uploaded: {item_name}")

            logger.info(f"Uploaded {uploaded_count} file(s) to Drive folder")
            return True

        except Exception as e:
            logger.error(f"Error in upload_rendered_files: {e}", exc_info=True)
            return False

    def move_queued_to_processed(self, job_folder, rendered_output_folder_path):
        """
        Moves photo job folder from Queued to Complte, then uploads new photo files into that folder.

        Args:
            job_folder: Job folder dict from Google Drive with 'id' and 'name'
            rendered_output_folder_path: Local path to folder containing rendered output files

        Returns:
            bool: True if both operations succeeded, False otherwise
        """
        try:
            logger.info(f"Moving job folder '{job_folder['name']}' from Queued to Complete")
            move_success = self.move_file(
                file_id=job_folder['id'],
                old_folder_id=self.photo_queued_id,
                new_folder_id=self.photo_complete_id
            )

            if not move_success:
                logger.error("Failed to move job folder to Complete")
                return False

            logger.info("Job folder moved to Complete")

            logger.info("Uploading rendered files into job folder")
            upload_success = self.upload_rendered_files(rendered_output_folder_path, job_folder['id'])

            if not upload_success:
                logger.error("Failed to upload rendered files to job folder")
                return False

            logger.info(f"Uploaded rendered files to Processed/{job_folder['name']}")
            return True

        except Exception as e:
            logger.error(f"Error in move_queued_to_processed: {e}", exc_info=True)
            return False

    def move_file(self, file_id, old_folder_id, new_folder_id):
        """
        Moves a file from one folder to another.

        For Shared Drives, files must have exactly one parent. We atomically swap parents
        using addParents and removeParents in a single update call.

        Args:
            file_id: Google Drive file/folder ID to move
            old_folder_id: Current parent folder ID
            new_folder_id: Target parent folder ID

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not new_folder_id or not old_folder_id:
                logger.error(f"Missing folder IDs - old: {old_folder_id}, new: {new_folder_id}")
                return False

            logger.debug(f"Moving file {file_id} from {old_folder_id} to {new_folder_id}")

            self.service.files().update(
                fileId=file_id,
                addParents=new_folder_id,
                removeParents=old_folder_id,
                supportsAllDrives=True
            ).execute()

            logger.debug(f"Moved file {file_id} to folder {new_folder_id}")
            return True

        except HttpError as e:
            logger.error(f"Drive error moving file {file_id}: {e}")
            return False
        except Exception as e:
            logger.error(f"Unexpected error moving file {file_id}: {e}", exc_info=True)
            return False

    def delete_file(self, file_id):
        self.service.files().delete(fileId=file_id).execute()

    def get_queued_ids(self) -> dict[str, object]:
        try:
            queued_items = self.list_items_in_folder(self.photo_queued_id)
            ids = {}
            for item in queued_items:
                if not item.get("mimeType") == "application/vnd.google-apps.folder":
                    ids[item['id']] = item
            return ids
        except Exception as e:
            logger.error(f"Failed to check Queued: {str(e)}")
            return None

    def check_queued(self):
        try:
            queued_items = self.list_items_in_folder(self.photo_queued_id)
            for items in queued_items:
                if not items.get("mimeType") == "application/vnd.google-apps.folder":
                    continue

        except Exception as e:
            logger.error(f"Failed to check Queued: {str(e)}")
            return None
        
                    
def get_drive_service():
    """Return MockDriveService if USE_MOCK_DRIVE is set, otherwise real DriveService."""
    # import os as _os
    # if _os.getenv("USE_MOCK_DRIVE", default=False):
    #     import sys
    #     if "/app" not in sys.path:
    #         sys.path.insert(0, "/app")
    #     from mock_drive.filesystem_drive import MockDriveService
    #     return MockDriveService(base_path=_os.getenv("MOCK_DRIVE_PATH", "/mock-drive-data"))
    return DriveService()