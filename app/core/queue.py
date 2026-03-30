from redis import Redis
import logging
import json

from app.models.job import Job, JobStatus
from app.core.settings import settings

logger = logging.getLogger("PhotoTransformer")

class Queue:
    def __init__(self):
        self.redis = Redis(host=settings.REDIS_HOST, port=settings.REDIS_PORT, decode_responses=True)
        self.size = 0

        if self.redis:
            logger.info(f'QUEUE: Connected to redis instance on host: {settings.REDIS_HOST}, port: {settings.REDIS_PORT}')
        else:
            logger.error(f'Failed to connect to redis instance on host: {settings.REDIS_HOST}, port: {settings.REDIS_PORT}')


    def putJob(self, job: Job):
        json_str = json.dumps(job.toJson())
        self.redis.hset(f"job:{job.id}", mapping=job.toJson())
        self.redis.rpush(f"queue:queued", job.id)
        self.size += 1

    def getJob(self, jobId: str) -> Job:
        jobDict = json.loads(self.redis.hgetall(f"job:{jobId}"))
        return Job(jobDict=jobDict)
    
    def isQueued(self, jobId: str) -> bool:
        """
        Returns True if job exisits and status is QUEUED or PROCESSING, False otherwise
        """
        jobDict = self.redis.hgetall(f"job:{jobId}")
        if not jobDict:
            return False
        
        job = Job(jobDict=jobDict)
        return job.status in [JobStatus.QUEUED, JobStatus.PROCESSING]
    
    def filterNew(self, ids: dict[str, object]) -> dict[str, object]:
        """
        Filters a list of drive folder items and returns Non-Queued Items
        """
        if not ids:
            return {}
        
        newIds = {}
        for id in ids.keys():
            if not self.redis.hexists(f"job:{id}", "id"):
                newIds[id] = ids[id]
        return newIds
    
    def _size(self):
        return self.redis.zlexcount()
    
    def completeJob(self, job: Job):
        self.redis.rpush("queue:complete", job.id)
        self.updateJob(job)
        
    def updateJob(self, job: Job):
        # json_str = json.dumps(job.toJson())
        self.redis.hset(f"job:{job.id}", mapping=job.toJson())

    def popJob(self):
        """
        Returns id of the top priority job
        type: Optional filter by job type
        """
        jobid = self.redis.lpop("queue:queued")

        if not jobid:
            return None

        jobDict = self.redis.hgetall(f"job:{jobid}")
        self.size -= 1

        return Job(jobDict=jobDict)
    