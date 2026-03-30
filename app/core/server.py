import os 
from fastapi import FastAPI
import logging
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime

from app.core.drive import get_drive_service, DriveService
from app.models.job import Job, JobType, JobStatus
from app.core.queue import Queue
from app.core.lensCorrection import apply_lens_correction
from app.api.photos import photos
from app.api.videos import videos

logger = logging.getLogger(name="PhotoTransformer")

class Manager:

    def __init__(self, driveService: DriveService, queueDriveService: DriveService, queue: Queue, shutdown_event: threading.Event):
        self.driveService = driveService
        self.queueDriveService = queueDriveService
        self.queue = queue
        self.shutdown_event = shutdown_event

    # TODO: Once job completes or errors, update redis, upload to completed drive
    def lenCorrectionLoop(self):
        while not self.shutdown_event.is_set():
            try: 
                job = self.queue.popJob()

                if job:
                    job.status = JobStatus.PROCESSING
                    job.started_at = datetime.now().isoformat()
                    logger.info(f"Job {job.id} removed from queue, running job")
                    logger.info(f'Downloading file from drive...')
                    self.driveService.download_file(file_id=job.id, local_path=f"/data/downloads/{job.id}/{job.name}")
                    completePath = f"/data/complete/{job.id}/{job.name}"

                    logger.info(f"Applying lense correction to file...")
                    apply_lens_correction(f"/data/downloads/{job.id}/{job.name}", completePath)

                    if not os.path.exists(completePath):
                        raise Exception(f"Error for Job {job.id}, output image not found at: {completePath}")
                    
                    logger.info(f"Output present, uploading img to drive...")
                    self.driveService.upload_file(completePath, self.driveService.photo_complete_id)

                    #TODO: if complete delete original graphic or move

                    logger.info(f"Img uploaded to PhotoComplete drive folder, updating queue.")
                    job.completed_at = datetime.now().isoformat()
                    job.status = JobStatus.COMPLETED
                    self.queue.completeJob(job)

                    
                else:
                    logger.info(f"No job in queue, pop empty.")
                    time.sleep(10)
            except Exception as e:
                logger.error(f"Error in lensCorrectionLoop: {str(e)}")
                
                if job:
                    job.error = str(e)
                    job.status = JobStatus.ERROR
                    job.completed_at = datetime.now().isoformat()
                    self.queue.completeJob(job)
                time.sleep(10)
                

    def DriveQueueLoop(self) -> None:
        while not self.shutdown_event.is_set():
            logger.info(f"Checking drive Queue folder")
            queuedItems = self.queueDriveService.get_queued_ids()
            newItems= self.queue.filterNew(queuedItems)

            for key, value in newItems.items():
                logger.info(f"New item in Queue - Creating Job: {key}")
                job = Job(driveId=value.get('id'), type=JobType.LENS_FILTER, name=value.get('name'))
                self.queue.putJob(job)

            logger.info(f"Finished Queue Check - Size: {self.queue.size}")
            time.sleep(10)

@asynccontextmanager
async def lifecycle(app: FastAPI):

    queueService = Queue()
    driveService = get_drive_service()
    queueDriveService = get_drive_service()

    shutdown_event = threading.Event()
    app.state.shutdown_event = shutdown_event

    manager = Manager(driveService=driveService, queueDriveService=queueDriveService, queue=queueService, shutdown_event=shutdown_event)
    app.state.manager = manager


    threads = [
        threading.Thread(
            target=manager.DriveQueueLoop, 
            daemon=True
        ),
        threading.Thread(
            target=manager.lenCorrectionLoop,
            daemon=True
        )
    ]

    for t in threads:
        t.start()

    yield
    shutdown_event.set()

def create_app() -> FastAPI:

    app = FastAPI(title="PhotoTransformer", lifespan=lifecycle)

    # app.add_middleware(CORSMiddleware(
    #     allow_origins=["*"],
    #     allow_headers=["*"],
    # ))

    app.include_router(photos)
    app.include_router(videos)

    return app