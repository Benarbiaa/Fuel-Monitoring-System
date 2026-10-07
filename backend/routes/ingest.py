from fastapi import APIRouter, HTTPException, Request

from backend.schemas import FuelData as FuelDataSchema, IngestAcceptedResponse
from kafka_common.readings_producer import DeliveryError

# How long a request waits for Kafka to confirm the reading before
# answering 503. Short enough that a client isn't left hanging when the
# broker is down, long enough for a healthy broker (normally milliseconds).
KAFKA_CONFIRM_TIMEOUT_SECONDS = 5.0

router = APIRouter()


@router.post("/", response_model=IngestAcceptedResponse, status_code=202)
def ingest_fuel_data(payload: FuelDataSchema, request: Request):
    """
    Accepts a fuel reading and publishes it to Kafka (fuel.readings.raw)
    for the ingestion consumer to actually validate-and-store.

    This endpoint deliberately does NOT write to the database anymore
    (see docs/adr/0001-kafka-ingestion.md, Phase 3). A 202 response means
    "accepted for processing", not "stored" — the write happens moments
    later, asynchronously, in ingestion_consumer/main.py. This mirrors
    exactly what the station simulator's Kafka sink has done since Phase 1;
    /ingest is now just a second producer into the same pipeline, using
    the same message key scheme so HTTP-sourced and simulator-sourced
    readings for the same tank stay correctly ordered on the same
    partition.

    The 202 is only sent once Kafka has confirmed it stored the reading.
    If Kafka is down or too slow, the client gets a 503 and knows to retry,
    instead of a 202 for a reading that would later be dropped from the
    producer's local queue without anyone being told.
    """
    try:
        request.app.state.kafka_producer.send_and_wait(
            station_id=payload.station_id,
            fuel_type=payload.fuel_type,
            # mode="json" converts non-JSON-native types (here, `timestamp`,
            # a datetime) into JSON-safe values (an ISO 8601 string) before
            # this dict gets handed to json.dumps() inside the producer.
            # payload.dict()/.model_dump() alone would leave it as a Python
            # datetime object, which json.dumps() cannot serialize —
            # this was the cause of the 500 error.
            payload=payload.model_dump(mode="json"),
            timeout=KAFKA_CONFIRM_TIMEOUT_SECONDS,
        )
    except (BufferError, DeliveryError):
        # BufferError: the producer's local queue is full. DeliveryError:
        # Kafka rejected the reading or didn't confirm it in time. Either
        # way it isn't safely stored, so say so and let the client retry.
        raise HTTPException(
            status_code=503,
            detail="Ingestion pipeline is temporarily unavailable, please retry.",
        )

    return IngestAcceptedResponse(
        station_id=payload.station_id,
        fuel_type=payload.fuel_type,
        timestamp=payload.timestamp,
    )