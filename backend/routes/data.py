from datetime import datetime, timezone
from typing import List, Literal, Optional

from fastapi import APIRouter, Query, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func
from backend.schemas import AlertResponse, FuelData, FuelDataResponse
from backend.database import models
from backend.database.database import get_db

router = APIRouter()


@router.get("/stations")
def get_stations(db: Session = Depends(get_db)):
    """Get all stations that have been registered via ingestion."""
    stations = db.query(models.Station).all()
    return [
        {
            "station_id": s.station_id,
            "company": s.company,
            "location": s.location,
        }
        for s in stations
    ]







@router.get("/current", response_model=List[FuelDataResponse])
def get_current(station_id: str = Query(...), db: Session = Depends(get_db)):
    # Get the latest record for each fuel_type for the station
    subquery = db.query(
        models.FuelData.fuel_type,
        func.max(models.FuelData.timestamp).label('max_ts')
    ).filter(models.FuelData.station_id == station_id).group_by(models.FuelData.fuel_type).subquery()
    
    results = db.query(models.FuelData).join(
        subquery,
        (models.FuelData.fuel_type == subquery.c.fuel_type) & (models.FuelData.timestamp == subquery.c.max_ts)
    ).all()
    
    return results


@router.get("/history", response_model=List[FuelDataResponse])
def get_history(
    station_id: str = Query(...),
    fuel_type: Optional[Literal["Gasoil50", "SansPlomb"]] = Query(None),
    limit: int = Query(500, le=2000),
    db: Session = Depends(get_db),
):
    query = db.query(models.FuelData).filter(models.FuelData.station_id == station_id)
    if fuel_type:
        query = query.filter(models.FuelData.fuel_type == fuel_type)
    results = query.order_by(models.FuelData.timestamp.desc()).limit(limit).all()
    return results[::-1]  # Return in ascending order


@router.get("/alerts", response_model=List[AlertResponse])
def get_alerts(
    station_id: Optional[str] = Query(None),
    severity: Optional[Literal["info", "warning", "critical"]] = Query(None),
    alert_type: Optional[Literal["LOW_STOCK", "PRICE_ANOMALY", "HIGH_CONSUMPTION", "STATION_CRITICAL", "RESTOCK"]] = Query(None),
    limit: int = Query(50, le=500),
    db: Session = Depends(get_db),
):
    query = db.query(models.Alert)
    if station_id:
        query = query.filter(models.Alert.station_id == station_id)
    if severity:
        query = query.filter(models.Alert.severity == severity)
    if alert_type:
        query = query.filter(models.Alert.alert_type == alert_type)
    results = query.order_by(models.Alert.timestamp.desc()).limit(limit).all()
    return results





@router.get("/companies")
def get_companies(db: Session = Depends(get_db)):
    """Get all unique companies from registered stations."""
    companies = db.query(models.Station.company).distinct().all()
    return [c[0] for c in companies if c[0]]