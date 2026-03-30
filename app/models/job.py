from enum import Enum
from datetime import datetime

class JobStatus(Enum):
    QUEUED = 1
    PROCESSING = 2
    ERROR = 3
    COMPLETED = 4

class JobType(Enum):
    UNKNOWN = 0
    LENS_FILTER = 1
    VIDEO_RESIZE = 2

class Job:

    id: str = ""

    status: JobStatus = JobStatus.QUEUED
    type: JobType = JobType.UNKNOWN

    name: str = ""

    error: str = ""
    completed_at: str = ""
    queued_at: str = ""
    started_at: str = ""

    def __init__(self, driveId=None, type=None, name=None, jobDict=None):
        if jobDict:
            self.__init_job_dict__(jobDict=jobDict)
        else:
            self.id = driveId
            self.status = JobStatus.QUEUED
            self.type = type

            self.name = name
            self.error = ""
            self.queued_at = datetime.now().isoformat()
            self.started_at = ""
            self.completed_at = ""

    def __init_job_dict__(self, jobDict):
        self.id = jobDict.get("id")
        self.type = JobType(int(jobDict.get("type")))
        self.status = JobStatus(int(jobDict.get("status")))

        self.name = jobDict.get('name')

        self.error = jobDict.get("error")
        self.queued_at = jobDict.get("queued_at")
        self.started_at = jobDict.get("started_at")
        self.completed_at = jobDict.get("completed_at")


    def toJson(self):
        return {
            "id": self.id,
            "name": self.name,
            "status": self.status.value,
            "type": self.type.value,
            "error": self.error,
            "completed_at": self.completed_at,
            "started_at": self.started_at
        }


