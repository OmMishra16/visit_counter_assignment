from fastapi import APIRouter, HTTPException, Depends, Query
from typing import Dict, Any
from ....services.visit_counter import VisitCounterService
from ....schemas.counter import VisitCount

router = APIRouter()

# Dependency to get VisitCounterService instance
def get_visit_counter_service():
    return VisitCounterService()

@router.post("/visit/{page_id}")
async def record_visit(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Record a visit for a website"""
    try:
        await counter_service.increment_visit_redis(page_id)
        return {"status": "success", "message": f"Visit recorded for page {page_id}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/visits/{page_id}", response_model=VisitCount)
async def increment_visit_counter(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Increment the visit counter for a specific page."""
    try:
        await counter_service.increment_visit_redis(page_id)
        count = await counter_service.get_visit_count_redis(page_id)
        return VisitCount(visits=count, served_via="redis")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/visits/{page_id}", response_model=VisitCount)
async def get_visits(
    page_id: str,
    counter_service: VisitCounterService = Depends(get_visit_counter_service)
):
    """Get visit count for a website"""
    try:
        count = await counter_service.get_visit_count_redis(page_id)
        return VisitCount(visits=count, served_via="redis")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))