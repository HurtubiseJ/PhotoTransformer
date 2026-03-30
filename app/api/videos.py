from fastapi import APIRouter, Request

videos = APIRouter(prefix="/videos")

@videos.post("/resize")
def resizeVideo(request: Request):
    pass