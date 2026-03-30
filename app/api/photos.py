from fastapi import APIRouter, Request

photos = APIRouter(prefix="/photos")


@photos.post("/chromatic-abberation")
def applyChromaticAbberation(request: Request):
    pass

