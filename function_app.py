import azure.functions as func
import pymongo
import os
import json
import logging
import re
from datetime import datetime, timedelta
from azure.storage.blob import BlobServiceClient, StandardBlobTier
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from openai import AzureOpenAI
import requests

app = func.FunctionApp()

BOOKING_ID_PATTERN = r"^BT-\d{4}-\d{8}$"


# =========================
# COMMON HELPERS
# =========================
def utc_now():
    return datetime.utcnow()


def iso_utc(dt: datetime) -> str:
    return dt.isoformat() + "Z"


def build_unique_raw_blob_name(booking_id: str, dt: datetime) -> str:
    # Example:
    # 2026/04/15/BT-2026-00000001_20260415T071530123456Z.json
    date_folder = dt.strftime("%Y/%m/%d")
    timestamp_part = dt.strftime("%Y%m%dT%H%M%S%fZ")
    return f"{date_folder}/{booking_id}_{timestamp_part}.json"



def build_failed_booking_id(booking_data: dict, dt: datetime) -> str:
    """
    Builds a safe, unique Cosmos _id for FailedBookings.

    Invalid booking IDs like BT-2025-00a21 should not be used directly
    as the failed record _id. We keep the original bookingId in
    bookingId/originalBookingId and use this generated ID as _id.
    """
    original_booking_id = booking_data.get("bookingId")

    if original_booking_id:
        original_booking_id = str(original_booking_id).strip().upper()
    else:
        original_booking_id = "UNKNOWN"

    safe_booking_part = re.sub(r"[^A-Z0-9\-]", "_", original_booking_id)
    timestamp_part = dt.strftime("%Y%m%dT%H%M%S%fZ")

    return f"FAILED_{safe_booking_part}_{timestamp_part}"


def to_list(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        return [value]
    return []


def get_nested_value(data: dict, path: list):
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def get_booking_origin_destination(booking_data: dict):
    origin = (
        booking_data.get("origin")
        or get_nested_value(booking_data, ["travelDetails", "origin"])
    )

    destination = (
        booking_data.get("destination")
        or get_nested_value(booking_data, ["travelDetails", "destination"])
    )

    if isinstance(origin, str):
        origin = origin.strip()

    if isinstance(destination, str):
        destination = destination.strip()

    return origin, destination


def set_booking_origin_destination(booking_data: dict, origin=None, destination=None):
    """
    Updates origin/destination safely.
    If travelDetails exists, updates inside travelDetails.
    Otherwise updates top-level fields.
    """
    if origin is not None:
        origin = str(origin).strip()
        if isinstance(booking_data.get("travelDetails"), dict):
            booking_data["travelDetails"]["origin"] = origin
        else:
            booking_data["origin"] = origin

    if destination is not None:
        destination = str(destination).strip()
        if isinstance(booking_data.get("travelDetails"), dict):
            booking_data["travelDetails"]["destination"] = destination
        else:
            booking_data["destination"] = destination

    return booking_data


def validate_review_scope(booking_data: dict) -> list:
    """
    Deterministic validation for current review scope only:
    1. bookingId validation
    2. origin validation
    3. destination validation

    AI should only explain these deterministic errors. AI should not decide validity.
    """
    errors = []

    booking_id = booking_data.get("bookingId")

    if not booking_id:
        errors.append("bookingId missing")
    else:
        booking_id = str(booking_id).strip().upper()

        if not re.match(BOOKING_ID_PATTERN, booking_id):
            errors.append("Invalid bookingId format")
        else:
            year = int(booking_id.split("-")[1])
            if year != utc_now().year:
                errors.append("Invalid bookingId year")

    origin, destination = get_booking_origin_destination(booking_data)

    if not origin:
        errors.append("Origin missing")

    if not destination:
        errors.append("Destination missing")

    if origin and destination and origin.lower() == destination.lower():
        errors.append("Origin and destination cannot be same")

    return errors


# =========================
# INGEST BOOKING API
# =========================
@app.function_name(name="ingest_booking")
@app.route(
    route="ingest-booking",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def ingest_booking(req: func.HttpRequest) -> func.HttpResponse:
    """
    Accepts booking JSON and pushes it to Azure Service Bus.
    Minimal validation only, so faulty test bookings can be ingested
    and naturally fail in the processor / DLQ flow.
    """
    try:
        try:
            booking_data = req.get_json()
        except ValueError:
            return func.HttpResponse(
                json.dumps({"message": "Request body must be valid JSON"}),
                status_code=400,
                mimetype="application/json"
            )

        if not isinstance(booking_data, dict):
            return func.HttpResponse(
                json.dumps({"message": "JSON body must be an object"}),
                status_code=400,
                mimetype="application/json"
            )

        booking_id = booking_data.get("bookingId")
        if not booking_id:
            return func.HttpResponse(
                json.dumps({"message": "bookingId is required"}),
                status_code=400,
                mimetype="application/json"
            )

        booking_data["bookingId"] = str(booking_id).strip().upper()

        service_bus_connection = os.environ.get("ServiceBusConnection")
        queue_name = os.environ.get("INGESTION_QUEUE_NAME", "ingestion-queue")

        if not service_bus_connection:
            return func.HttpResponse(
                json.dumps({"message": "ServiceBusConnection app setting is missing"}),
                status_code=500,
                mimetype="application/json"
            )

        message_body = json.dumps(booking_data, default=str)

        with ServiceBusClient.from_connection_string(service_bus_connection) as client:
            with client.get_queue_sender(queue_name=queue_name) as sender:
                sender.send_messages(
                    ServiceBusMessage(
                        message_body,
                        content_type="application/json",
                        subject="travel-booking"
                    )
                )

        logging.info("Booking pushed to Service Bus. bookingId=%s", booking_data["bookingId"])

        return func.HttpResponse(
            json.dumps({
                "message": "Booking sent to Service Bus",
                "bookingId": booking_data["bookingId"],
                "queueName": queue_name,
                "submittedAt": iso_utc(utc_now())
            }),
            status_code=202,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error("INGEST API ERROR: %s", str(e))
        return func.HttpResponse(
            json.dumps({
                "message": "Failed to send booking to Service Bus",
                "error": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )


# =========================
# MAIN INGESTION FUNCTION
# =========================
@app.function_name(name="travel_booking_processor")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="ingestion-queue",
    connection="ServiceBusConnection"
)
def travel_booking_processor(msg: func.ServiceBusMessage):
    try:
        raw_body = msg.get_body().decode("utf-8")
        booking_data = json.loads(raw_body)

        booking_id = booking_data.get("bookingId")

        if not booking_id:
            raise Exception("bookingId missing")

        booking_id = str(booking_id).strip().upper()
        booking_data["bookingId"] = booking_id

        # Skip strict validation only if manually approved
        if not booking_data.get("validationOverride"):
            validation_errors = validate_review_scope(booking_data)
            if validation_errors:
                raise Exception("; ".join(validation_errors))

    except Exception as e:
        logging.error("VALIDATION ERROR: %s", str(e))
        raise

    try:
        now = utc_now()

        # Enrich payload for operational + analytical traceability
        booking_data["_id"] = booking_id
        booking_data["bookingId"] = booking_id
        booking_data["updatedAt"] = now
        booking_data["rawStoredAt"] = now
        booking_data["rawEventId"] = f"{booking_id}_{now.strftime('%Y%m%dT%H%M%S%fZ')}"

        client = pymongo.MongoClient(
            os.environ["CosmosDBConnection"],
            tls=True
        )

        db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
        col = db["Bookings"]

        col.update_one(
            {"_id": booking_id},
            {"$set": booking_data},
            upsert=True
        )

        logging.info("Booking stored in Bookings")

        # RAW STORAGE - ALWAYS NEW UNIQUE FILE
        blob_service = BlobServiceClient.from_connection_string(
            os.environ["AzureWebJobsStorage"]
        )

        raw_blob_name = build_unique_raw_blob_name(booking_id, now)
        raw_payload = json.dumps(booking_data, default=str)

        blob_service.get_blob_client(
            container="raw-data",
            blob=raw_blob_name
        ).upload_blob(raw_payload, overwrite=False)

        logging.info("Stored in raw-data: %s", raw_blob_name)

    except Exception as e:
        logging.error("PROCESS ERROR: %s", str(e))
        raise


# =========================
# DLQ PROCESSOR
# =========================
@app.function_name(name="dlq_processor")
@app.service_bus_queue_trigger(
    arg_name="msg",
    queue_name="ingestion-queue/$DeadLetterQueue",
    connection="ServiceBusConnection"
)
def dlq_processor(msg: func.ServiceBusMessage):
    """
    Reads messages from Service Bus DLQ and stores them into Cosmos DB FailedBookings.

    This version is defensive:
    - Invalid bookingId is not used as the Cosmos _id.
    - A generated failed_record_id is used as _id.
    - Original invalid bookingId is preserved for review and AI explanation.
    - Blob failure is logged, but does not block Cosmos failed-record creation.
    - Cosmos failure is raised, because otherwise the failed booking would be lost.
    """
    try:
        raw_body = msg.get_body().decode("utf-8")

        try:
            booking_data = json.loads(raw_body)
            if not isinstance(booking_data, dict):
                booking_data = {"rawMessage": raw_body}
        except Exception:
            booking_data = {"rawMessage": raw_body}

        now = utc_now()

        original_booking_id = booking_data.get("bookingId", "UNKNOWN")
        if original_booking_id:
            original_booking_id = str(original_booking_id).strip().upper()
        else:
            original_booking_id = "UNKNOWN"

        failed_record_id = build_failed_booking_id(booking_data, now)

        # Calculate deterministic validation errors safely.
        try:
            validation_errors = validate_review_scope(booking_data)
        except Exception as validation_ex:
            validation_errors = [
                f"Validation check failed inside DLQ processor: {str(validation_ex)}"
            ]

        if not validation_errors:
            validation_errors = ["Message moved to DLQ"]

        # Store failed raw payload in failed-data container.
        blob_name = None
        try:
            blob_service = BlobServiceClient.from_connection_string(
                os.environ["AzureWebJobsStorage"]
            )

            blob_name = (
                now.strftime("%Y/%m/%d/%H-%M-%S-%f")
                + f"_{failed_record_id}.json"
            )

            blob_service.get_blob_client(
                container="failed-data",
                blob=blob_name
            ).upload_blob(
                json.dumps(booking_data, default=str),
                overwrite=True,
                standard_blob_tier=StandardBlobTier.Cool
            )

            logging.info("Stored failed message in failed-data: %s", blob_name)

        except Exception as blob_ex:
            # Do not fail DLQ processing only because blob storage failed.
            # The failed booking can still be reviewed from Cosmos DB.
            logging.error("Failed to store DLQ message in Blob: %s", str(blob_ex))

        # Store failed record in Cosmos DB.
        try:
            client = pymongo.MongoClient(
                os.environ["CosmosDBConnection"],
                tls=True
            )

            db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
            col = db["FailedBookings"]

            failed_doc = {
                "_id": failed_record_id,
                "bookingId": original_booking_id,
                "originalBookingId": original_booking_id,
                "status": "FAILED",
                "reviewStatus": "PENDING",
                "originalMessage": booking_data,
                "pocEmail": booking_data.get("pocEmail"),
                "validationErrors": validation_errors,
                "failureReason": "; ".join(validation_errors),
                "failedBlobName": blob_name,
                "aiReviewStatus": "PENDING",
                "notificationStatus": "PENDING",
                "notified": False,
                "createdAt": now,
                "updatedAt": now
            }

            col.insert_one(failed_doc)

            logging.info(
                "DLQ message stored in FailedBookings. failedRecordId=%s, originalBookingId=%s",
                failed_record_id,
                original_booking_id
            )

            # Real-time AI Agent behavior: automatically review and notify.
            # AI/notification failure does not block DLQ processing.
            logging.info(
                "About to start automatic AI review. failedRecordId=%s",
                failed_record_id
            )

            try:
                col.update_one(
                    {"_id": failed_record_id},
                    {"$set": {
                        "aiReviewStatus": "IN_PROGRESS",
                        "aiReviewStartedAt": utc_now(),
                        "updatedAt": utc_now()
                    }}
                )

                ai_result = run_ai_review_and_notify_for_failed_doc(col, failed_doc)

                logging.info(
                    "Automatic AI review finished. failedRecordId=%s, result=%s",
                    failed_record_id,
                    json.dumps(ai_result, default=str)
                )

            except Exception as auto_ai_ex:
                # Do not block DLQ completion because AI/notification failed.
                # The failed booking is already safely stored in FailedBookings.
                logging.error(
                    "Automatic AI review/notification failed after DLQ insert. failedRecordId=%s, error=%s",
                    failed_record_id,
                    str(auto_ai_ex)
                )
                col.update_one(
                    {"_id": failed_record_id},
                    {"$set": {
                        "aiReviewStatus": "FAILED",
                        "aiReviewError": str(auto_ai_ex),
                        "notificationStatus": "SKIPPED_OR_FAILED",
                        "updatedAt": utc_now()
                    }}
                )

        except Exception as cosmos_ex:
            # If Cosmos write fails, raise so the DLQ message is not silently lost.
            logging.error("Failed to store DLQ message in Cosmos DB: %s", str(cosmos_ex))
            raise

    except Exception as e:
        logging.error("DLQ PROCESSOR ERROR: %s", str(e))
        raise


# =========================
# BULK APPROVAL / REJECTION API
# =========================
@app.function_name(name="review_bookings_bulk")
@app.route(
    route="review-bookings-bulk",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def review_bookings_bulk(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()

        poc_email = body.get("pocEmail")
        action = body.get("action")
        corrected_booking_id = body.get("correctedBookingId")
        corrected_origin = body.get("correctedOrigin")
        corrected_destination = body.get("correctedDestination")

        if not poc_email or not action:
            return func.HttpResponse("Missing fields", status_code=400)

        action = action.lower()

        client = pymongo.MongoClient(
            os.environ["CosmosDBConnection"],
            tls=True
        )

        db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
        failed_col = db["FailedBookings"]
        bookings_col = db["Bookings"]

        query = {
            "reviewStatus": "PENDING",
            "pocEmail": poc_email
        }

        failed_docs = list(failed_col.find(query))

        if not failed_docs:
            return func.HttpResponse("No bookings found", status_code=404)

        processed_count = 0

        if action == "approve":
            if not corrected_booking_id:
                return func.HttpResponse(
                    "correctedBookingId is required for approve",
                    status_code=400
                )

            corrected_booking_id = corrected_booking_id.strip().upper()

            if not re.match(BOOKING_ID_PATTERN, corrected_booking_id):
                return func.HttpResponse(
                    "Invalid correctedBookingId format",
                    status_code=400
                )

            year = int(corrected_booking_id.split("-")[1])
            if year != utc_now().year:
                return func.HttpResponse(
                    "Invalid correctedBookingId year",
                    status_code=400
                )

            for failed_doc in failed_docs:
                data = failed_doc.get("originalMessage", {})
                original_booking_id = data.get("bookingId") or failed_doc["_id"]

                data["_id"] = corrected_booking_id
                data["bookingId"] = corrected_booking_id
                data["originalBookingId"] = original_booking_id

                # Optional origin/destination corrections for the new validation scope
                if corrected_origin or corrected_destination:
                    data = set_booking_origin_destination(
                        data,
                        origin=corrected_origin,
                        destination=corrected_destination
                    )

                # Validate final data before moving to Bookings.
                # This prevents manual approval from moving a record with same/missing origin-destination.
                final_validation_errors = validate_review_scope(data)
                if final_validation_errors:
                    return func.HttpResponse(
                        json.dumps({
                            "message": "Corrected booking still has validation errors",
                            "originalBookingId": str(original_booking_id),
                            "correctedBookingId": corrected_booking_id,
                            "validationErrors": final_validation_errors
                        }),
                        status_code=400,
                        mimetype="application/json"
                    )

                data["validationOverride"] = True
                data["source"] = "MANUAL_APPROVAL"
                data["approvedAt"] = utc_now()
                data["approvedByPocEmail"] = poc_email
                data["updatedAt"] = utc_now()

                bookings_col.update_one(
                    {"_id": corrected_booking_id},
                    {"$set": data},
                    upsert=True
                )

                failed_col.delete_one({"_id": failed_doc["_id"]})

                processed_count += 1
                logging.info(
                    "Approved booking copied to Bookings and deleted from FailedBookings. "
                    "Original: %s, Corrected: %s",
                    original_booking_id,
                    corrected_booking_id
                )

            return func.HttpResponse(
                json.dumps({
                    "message": str(processed_count) + " bookings approved",
                    "correctedBookingId": corrected_booking_id
                }),
                mimetype="application/json"
            )

        elif action == "reject":
            for failed_doc in failed_docs:
                failed_col.delete_one({"_id": failed_doc["_id"]})
                logging.info("Rejected booking deleted from FailedBookings: %s", failed_doc["_id"])
                processed_count += 1

            return func.HttpResponse(
                json.dumps({
                    "message": str(processed_count) + " bookings rejected"
                }),
                mimetype="application/json"
            )

        else:
            return func.HttpResponse("Invalid action", status_code=400)

    except Exception as e:
        logging.error(str(e))
        return func.HttpResponse(str(e), status_code=500)


# =========================
# GET FAILED BOOKINGS
# =========================
@app.function_name(name="get_failed_bookings")
@app.route(
    route="get-failed",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def get_failed_bookings(req: func.HttpRequest) -> func.HttpResponse:
    try:
        poc_email = req.params.get("pocEmail")

        client = pymongo.MongoClient(
            os.environ["CosmosDBConnection"],
            tls=True
        )

        db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
        col = db["FailedBookings"]

        query = {
            "reviewStatus": "PENDING"
        }

        if poc_email:
            query["pocEmail"] = poc_email

        results = list(col.find(query).sort("createdAt", -1))

        for r in results:
            r["_id"] = str(r["_id"])

        return func.HttpResponse(
            json.dumps(results, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        return func.HttpResponse(str(e), status_code=500)


# =========================
# GET SUCCESSFUL BOOKINGS
# =========================
@app.function_name(name="get_successful_bookings")
@app.route(
    route="get-bookings",
    methods=["GET"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def get_successful_bookings(req: func.HttpRequest) -> func.HttpResponse:
    try:
        booking_id = req.params.get("bookingId")
        employee_id = req.params.get("employeeId")
        poc_email = req.params.get("pocEmail")
        limit = req.params.get("limit", "20")

        try:
            limit = int(limit)
        except Exception:
            limit = 20

        if limit < 1:
            limit = 1
        if limit > 100:
            limit = 100

        client = pymongo.MongoClient(
            os.environ["CosmosDBConnection"],
            tls=True
        )

        db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
        col = db["Bookings"]

        query = {}

        if booking_id:
            query["bookingId"] = booking_id.strip().upper()

        if employee_id:
            query["employee.employeeId"] = employee_id.strip()

        if poc_email:
            query["pocEmail"] = poc_email.strip()

        results = list(
            col.find(query).sort("updatedAt", -1).limit(limit)
        )

        for r in results:
            r["_id"] = str(r["_id"])

        return func.HttpResponse(
            json.dumps(results, default=str),
            mimetype="application/json"
        )

    except Exception as e:
        logging.error("GET BOOKINGS ERROR: %s", str(e))
        return func.HttpResponse(str(e), status_code=500)


# =========================
# AI AGENT HELPERS
# =========================
def normalize_ai_review(ai_result: dict) -> dict:
    confidence = ai_result.get("confidence", "Medium")

    if isinstance(confidence, (int, float)):
        if confidence >= 0.8:
            confidence = "High"
        elif confidence >= 0.5:
            confidence = "Medium"
        else:
            confidence = "Low"
    elif isinstance(confidence, str):
        confidence = confidence.strip().capitalize()
        if confidence not in ["High", "Medium", "Low"]:
            confidence = "Medium"
    else:
        confidence = "Medium"

    review_notes = ai_result.get("review_notes", [])
    if isinstance(review_notes, str):
        review_notes = [review_notes]
    elif not isinstance(review_notes, list):
        review_notes = []

    failed_rules = ai_result.get("failed_rules", [])
    if isinstance(failed_rules, str):
        failed_rules = [failed_rules]
    elif not isinstance(failed_rules, list):
        failed_rules = []

    suggested_action = ai_result.get("suggested_action", "MANUAL_REVIEW")

    # No APPROVE_SUGGESTION in failed-booking review stage.
    # Human approval is handled by review_bookings_bulk after deterministic validation.
    allowed_actions = [
        "REJECT_SUGGESTION",
        "NEEDS_CORRECTION",
        "MANUAL_REVIEW"
    ]

    if suggested_action not in allowed_actions:
        suggested_action = "MANUAL_REVIEW"

    return {
        "failure_summary": ai_result.get("failure_summary", ""),
        "failed_rules": failed_rules,
        "suggested_action": suggested_action,
        "business_explanation": ai_result.get("business_explanation", ""),
        "confidence": confidence,
        "review_notes": review_notes
    }


def get_failed_booking_review_system_instruction() -> str:
    return """
You are a Failed Booking Review Agent for a corporate travel booking data platform.

Current validation scope:
You only explain these validation areas:
1. bookingId validation
2. origin validation
3. destination validation

Do not validate or mention:
- cost
- flightCost
- hotelCost
- totalCost
- POC email
- return date
- booking type
- employee details
- hotel details

Return only valid JSON.

Return JSON in this exact structure:
{
  "failure_summary": "",
  "failed_rules": [],
  "suggested_action": "",
  "business_explanation": "",
  "confidence": "",
  "review_notes": []
}

Rules:
- failure_summary must be a string.
- failed_rules must be an array of strings.
- suggested_action must be one of:
  NEEDS_CORRECTION, MANUAL_REVIEW, REJECT_SUGGESTION.
- business_explanation must be a string.
- confidence must be exactly one of:
  High, Medium, Low.
- review_notes must be an array of strings.

BookingId validation rules:
- bookingId must be present.
- bookingId must match format BT-YYYY-00000000.
- Example valid bookingId: BT-2026-00001234.
- bookingId year must match currentProcessingYear.

Origin/destination validation rules:
- origin must be present.
- destination must be present.
- origin and destination must be different.

Decision rules:
- If bookingId format is invalid, use NEEDS_CORRECTION.
- If bookingId year does not match currentProcessingYear, use MANUAL_REVIEW.
- If origin or destination is missing, use NEEDS_CORRECTION.
- If origin and destination are same, use NEEDS_CORRECTION.
- If multiple critical issues exist, use MANUAL_REVIEW.
- If the value is clearly fake/test/random and cannot be trusted, use REJECT_SUGGESTION.

Do not approve, reject, delete, update, or move any booking record.
The final decision must be made by a human reviewer.
Do not generate a corrected bookingId yourself.
Only explain the expected format and correction needed.

Strict output rule:
The response must start with { and end with }.
Do not include markdown code blocks.
"""


def call_ai_review_model(booking_payload: dict) -> dict:
    ai_client = AzureOpenAI(
        azure_endpoint=os.environ["AZURE_OPENAI_ENDPOINT"],
        api_key=os.environ["AZURE_OPENAI_KEY"],
        api_version=os.environ.get("AZURE_OPENAI_API_VERSION", "2024-12-01-preview")
    )

    deployment = os.environ["AZURE_OPENAI_DEPLOYMENT"]

    ai_response = ai_client.chat.completions.create(
        model=deployment,
        messages=[
            {"role": "system", "content": get_failed_booking_review_system_instruction()},
            {"role": "user", "content": json.dumps(booking_payload, default=str)}
        ],
        temperature=0.1,
        response_format={"type": "json_object"}
    )

    raw_ai_result = json.loads(ai_response.choices[0].message.content)
    return normalize_ai_review(raw_ai_result)


def build_ai_review_payload_from_failed_doc(failed_booking: dict) -> dict:
    """Build safe payload for AI Agent. Current scope: bookingId + origin + destination only."""
    original_message = failed_booking.get("originalMessage", {})
    source_data = original_message if isinstance(original_message, dict) and original_message else failed_booking

    calculated_validation_errors = validate_review_scope(source_data)

    stored_validation_errors = (
        to_list(failed_booking.get("validationErrors"))
        or to_list(failed_booking.get("failedRules"))
        or to_list(failed_booking.get("errors"))
        or to_list(failed_booking.get("failureReasons"))
    )

    final_validation_errors = calculated_validation_errors or stored_validation_errors
    origin, destination = get_booking_origin_destination(source_data)

    return {
        "bookingId": source_data.get("bookingId") or failed_booking.get("bookingId") or failed_booking.get("_id"),
        "origin": origin,
        "destination": destination,
        "currentProcessingYear": utc_now().year,
        "validationErrors": final_validation_errors
    }


def build_ai_review_doc(clean_ai_result: dict) -> dict:
    deployment = os.environ.get("AZURE_OPENAI_DEPLOYMENT", "unknown")
    return {
        "status": "COMPLETED",
        "failureSummary": clean_ai_result.get("failure_summary"),
        "failedRules": clean_ai_result.get("failed_rules", []),
        "suggestedAction": clean_ai_result.get("suggested_action"),
        "businessExplanation": clean_ai_result.get("business_explanation"),
        "confidence": clean_ai_result.get("confidence"),
        "reviewNotes": clean_ai_result.get("review_notes", []),
        "reviewedAt": utc_now(),
        "modelName": deployment,
        "promptVersion": "failed-booking-review-v2-bookingid-origin-destination"
    }


def send_failed_booking_alert(alert_payload: dict) -> bool:
    """Send alert payload to Logic App HTTP trigger. Logic App can send Email and/or Teams."""
    webhook_url = os.environ.get("FAILED_BOOKING_ALERT_WEBHOOK_URL")
    if not webhook_url:
        logging.info("FAILED_BOOKING_ALERT_WEBHOOK_URL is not configured. Skipping alert notification.")
        return False
    try:
        response = requests.post(webhook_url, json=alert_payload, timeout=15)
        response.raise_for_status()
        logging.info("Failed booking alert sent successfully for bookingId=%s", alert_payload.get("bookingId"))
        return True
    except Exception as alert_ex:
        logging.error("Failed to send failed booking alert: %s", str(alert_ex))
        return False


def build_alert_payload(failed_doc: dict, booking_payload: dict, ai_review_doc: dict) -> dict:
    reviewed_at = ai_review_doc.get("reviewedAt")
    if isinstance(reviewed_at, datetime):
        reviewed_at = iso_utc(reviewed_at)
    elif reviewed_at is None:
        reviewed_at = iso_utc(utc_now())
    else:
        reviewed_at = str(reviewed_at)

    return {
        "failedRecordId": str(failed_doc.get("_id")),
        "bookingId": str(booking_payload.get("bookingId") or failed_doc.get("bookingId") or "UNKNOWN"),
        "origin": booking_payload.get("origin"),
        "destination": booking_payload.get("destination"),
        "validationErrors": booking_payload.get("validationErrors", []),
        "failureSummary": ai_review_doc.get("failureSummary"),
        "suggestedAction": ai_review_doc.get("suggestedAction"),
        "confidence": ai_review_doc.get("confidence"),
        "reviewNotes": ai_review_doc.get("reviewNotes", []),
        "reviewedAt": reviewed_at,
        "promptVersion": ai_review_doc.get("promptVersion"),
        "modelName": ai_review_doc.get("modelName")
    }


def run_ai_review_and_notify_for_failed_doc(failed_col, failed_doc: dict) -> dict:
    """
    Automatically run AI review for a failed booking.

    Important production change:
    - AI review still runs immediately for each failed booking.
    - Email is NOT sent here anymore.
    - Email notification is batched by failed_booking_summary_alert_timer.
    This prevents inbox flooding during bulk failures.
    """
    failed_record_id = failed_doc["_id"]
    booking_payload = build_ai_review_payload_from_failed_doc(failed_doc)

    try:
        clean_ai_result = call_ai_review_model(booking_payload)
        ai_review_doc = build_ai_review_doc(clean_ai_result)

        failed_col.update_one(
            {"_id": failed_record_id},
            {"$set": {
                "aiReview": ai_review_doc,
                "aiReviewStatus": "COMPLETED",
                "validationErrors": booking_payload.get("validationErrors", []),
                "notificationStatus": "PENDING",
                "notified": False,
                "updatedAt": utc_now()
            }}
        )
        logging.info("AI review completed and queued for summary notification. failedRecordId=%s", failed_record_id)
        return {
            "bookingPayloadSentToAI": booking_payload,
            "aiReview": ai_review_doc,
            "notificationQueued": True
        }

    except Exception as ai_ex:
        logging.error("AI review failed for failedRecordId=%s. Error=%s", failed_record_id, str(ai_ex))
        failed_col.update_one(
            {"_id": failed_record_id},
            {"$set": {
                "aiReviewStatus": "FAILED",
                "aiReviewError": str(ai_ex),
                "notificationStatus": "PENDING",
                "notified": False,
                "updatedAt": utc_now()
            }}
        )
        return {
            "bookingPayloadSentToAI": booking_payload,
            "aiReviewError": str(ai_ex),
            "notificationQueued": True
        }


def html_escape(value):
    """Safely escape values before placing them inside HTML email content."""
    if value is None:
        return ""

    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
        .replace("'", "&#39;")
    )


def build_failed_booking_summary_payload(failed_docs: list, batch_id: str, window_start: datetime, window_end: datetime) -> dict:
    """
    Builds a structured HTML summary email payload for failed bookings.

    Production behavior:
    - AI review runs immediately for each failed booking.
    - Email notification is batched to prevent inbox flooding.
    - This function prepares one clean HTML email with summary cards and tables.
    """
    validation_counts = {}
    suggested_action_counts = {}
    ai_status_counts = {}
    examples = []

    for doc in failed_docs:
        ai_status = doc.get("aiReviewStatus", "UNKNOWN")
        ai_status_counts[ai_status] = ai_status_counts.get(ai_status, 0) + 1

        validation_errors = to_list(doc.get("validationErrors"))
        if not validation_errors:
            validation_errors = [doc.get("failureReason") or "Unknown failure"]

        for err in validation_errors:
            err = str(err)
            validation_counts[err] = validation_counts.get(err, 0) + 1

        ai_review = doc.get("aiReview", {}) if isinstance(doc.get("aiReview"), dict) else {}
        suggested_action = ai_review.get("suggestedAction") or "MANUAL_REVIEW"
        suggested_action_counts[suggested_action] = suggested_action_counts.get(suggested_action, 0) + 1

        if len(examples) < 10:
            original = doc.get("originalMessage", {}) if isinstance(doc.get("originalMessage"), dict) else {}
            origin, destination = get_booking_origin_destination(original or doc)
            examples.append({
                "failedRecordId": str(doc.get("_id")),
                "bookingId": str(doc.get("bookingId") or "UNKNOWN"),
                "origin": origin or "",
                "destination": destination or "",
                "failureSummary": ai_review.get("failureSummary") or doc.get("aiReviewError") or doc.get("failureReason") or "Failed booking requires review",
                "suggestedAction": suggested_action,
                "validationErrors": validation_errors
            })

    total_failures = len(failed_docs)
    needs_correction_count = suggested_action_counts.get("NEEDS_CORRECTION", 0)
    manual_review_count = suggested_action_counts.get("MANUAL_REVIEW", 0)
    reject_suggestion_count = suggested_action_counts.get("REJECT_SUGGESTION", 0)
    completed_ai_count = ai_status_counts.get("COMPLETED", 0)
    failed_ai_count = ai_status_counts.get("FAILED", 0)

    def build_count_rows(counts: dict, empty_label: str = "None") -> str:
        if not counts:
            return f"""
            <tr>
                <td style="border:1px solid #d1d5db;padding:8px;">{html_escape(empty_label)}</td>
                <td style="border:1px solid #d1d5db;padding:8px;text-align:right;">0</td>
            </tr>
            """

        rows = []
        for key, value in sorted(counts.items(), key=lambda x: str(x[0])):
            rows.append(f"""
            <tr>
                <td style="border:1px solid #d1d5db;padding:8px;">{html_escape(key)}</td>
                <td style="border:1px solid #d1d5db;padding:8px;text-align:right;">{value}</td>
            </tr>
            """)
        return "".join(rows)

    validation_rows = build_count_rows(dict(sorted(validation_counts.items(), key=lambda x: x[1], reverse=True)))
    action_rows = build_count_rows(dict(sorted(suggested_action_counts.items(), key=lambda x: x[1], reverse=True)))
    ai_status_rows = build_count_rows(dict(sorted(ai_status_counts.items(), key=lambda x: str(x[0]))))

    example_rows = []
    for idx, item in enumerate(examples, start=1):
        example_rows.append(f"""
        <tr>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;">{idx}</td>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;white-space:nowrap;">{html_escape(item.get('bookingId'))}</td>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;">{html_escape(item.get('origin'))}</td>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;">{html_escape(item.get('destination'))}</td>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;white-space:nowrap;">{html_escape(item.get('suggestedAction'))}</td>
            <td style="border:1px solid #d1d5db;padding:8px;vertical-align:top;">{html_escape(item.get('failureSummary'))}</td>
        </tr>
        """)

    if not example_rows:
        example_rows.append("""
        <tr>
            <td colspan="6" style="border:1px solid #d1d5db;padding:8px;">No examples available.</td>
        </tr>
        """)

    email_subject = f"Failed Booking Summary Alert - {total_failures} failure(s)"

    email_body = f"""
<html>
<body style="margin:0;padding:0;background-color:#f8fafc;font-family:Arial,Helvetica,sans-serif;color:#1f2937;line-height:1.45;">
    <div style="max-width:980px;margin:0 auto;background-color:#ffffff;padding:22px;border:1px solid #e5e7eb;">
        <h2 style="margin:0 0 8px 0;color:#0f172a;font-size:22px;">Failed Booking Summary Alert</h2>
        <p style="margin:0 0 18px 0;color:#475569;font-size:13px;">
            AI-reviewed failed booking summary generated by the Failed Booking Review Agent.
        </p>

        <table style="border-collapse:collapse;width:100%;margin-bottom:18px;">
            <tr>
                <td style="border:1px solid #d1d5db;padding:10px;background-color:#eff6ff;width:25%;">
                    <div style="font-size:12px;color:#1d4ed8;font-weight:bold;">Total Failed Bookings</div>
                    <div style="font-size:24px;font-weight:bold;color:#0f172a;">{total_failures}</div>
                </td>
                <td style="border:1px solid #d1d5db;padding:10px;background-color:#ecfdf5;width:25%;">
                    <div style="font-size:12px;color:#047857;font-weight:bold;">AI Reviews Completed</div>
                    <div style="font-size:24px;font-weight:bold;color:#0f172a;">{completed_ai_count}</div>
                </td>
                <td style="border:1px solid #d1d5db;padding:10px;background-color:#fff7ed;width:25%;">
                    <div style="font-size:12px;color:#c2410c;font-weight:bold;">Needs Correction</div>
                    <div style="font-size:24px;font-weight:bold;color:#0f172a;">{needs_correction_count}</div>
                </td>
                <td style="border:1px solid #d1d5db;padding:10px;background-color:#fef2f2;width:25%;">
                    <div style="font-size:12px;color:#b91c1c;font-weight:bold;">Manual Review</div>
                    <div style="font-size:24px;font-weight:bold;color:#0f172a;">{manual_review_count}</div>
                </td>
            </tr>
        </table>

        <table style="border-collapse:collapse;width:100%;margin-bottom:18px;">
            <tr>
                <td style="padding:8px 0;"><b>Time Window:</b> {html_escape(iso_utc(window_start))} to {html_escape(iso_utc(window_end))}</td>
            </tr>
            <tr>
                <td style="padding:8px 0;"><b>Batch ID:</b> {html_escape(batch_id)}</td>
            </tr>
        </table>

        <h3 style="margin:18px 0 8px 0;color:#0f172a;font-size:16px;">AI Review Status Counts</h3>
        <table style="border-collapse:collapse;width:100%;margin-bottom:16px;font-size:13px;">
            <tr style="background-color:#f3f4f6;">
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Status</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:right;">Count</th>
            </tr>
            {ai_status_rows}
        </table>

        <h3 style="margin:18px 0 8px 0;color:#0f172a;font-size:16px;">Suggested Action Counts</h3>
        <table style="border-collapse:collapse;width:100%;margin-bottom:16px;font-size:13px;">
            <tr style="background-color:#f3f4f6;">
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Suggested Action</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:right;">Count</th>
            </tr>
            {action_rows}
        </table>

        <h3 style="margin:18px 0 8px 0;color:#0f172a;font-size:16px;">Validation Error Counts</h3>
        <table style="border-collapse:collapse;width:100%;margin-bottom:16px;font-size:13px;">
            <tr style="background-color:#f3f4f6;">
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Validation Error</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:right;">Count</th>
            </tr>
            {validation_rows}
        </table>

        <h3 style="margin:18px 0 8px 0;color:#0f172a;font-size:16px;">Top Failed Booking Examples</h3>
        <table style="border-collapse:collapse;width:100%;margin-bottom:18px;font-size:13px;">
            <tr style="background-color:#f3f4f6;">
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">#</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Booking ID</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Origin</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Destination</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">Action</th>
                <th style="border:1px solid #d1d5db;padding:8px;text-align:left;">AI Summary</th>
            </tr>
            {''.join(example_rows)}
        </table>

        <div style="border-left:4px solid #f59e0b;background-color:#fffbeb;padding:12px;margin-bottom:16px;">
            <b>Action Required:</b><br/>
            Please open the FailedBookings review dashboard and process pending records.
        </div>

        <div style="border-left:4px solid #64748b;background-color:#f8fafc;padding:12px;margin-bottom:16px;color:#334155;">
            <b>Human-in-the-loop note:</b><br/>
            AI explains the issue and suggests action. Final approve/reject decision remains with the human reviewer.
        </div>

        <p style="font-size:12px;color:#64748b;margin-top:18px;">
            Additional counts: AI Review Failed = {failed_ai_count}, Reject Suggestion = {reject_suggestion_count}.<br/>
            This alert was generated automatically by the Failed Booking Review Agent.
        </p>
    </div>
</body>
</html>
"""

    return {
        "alertType": "FAILED_BOOKING_SUMMARY",
        "batchId": batch_id,
        "windowStart": iso_utc(window_start),
        "windowEnd": iso_utc(window_end),
        "totalFailures": total_failures,
        "aiStatusCounts": ai_status_counts,
        "validationErrorCounts": validation_counts,
        "suggestedActionCounts": suggested_action_counts,
        "examples": examples,
        "emailSubject": email_subject,
        "emailBody": email_body,
        "message": "Failed booking summary generated by AI-assisted review workflow. Please check FailedBookings dashboard for full details."
    }


def send_failed_booking_summary_alert(summary_payload: dict) -> bool:
    """Send one summarized alert to Logic App instead of one email per failed booking."""
    webhook_url = os.environ.get("FAILED_BOOKING_ALERT_WEBHOOK_URL")
    if not webhook_url:
        logging.info("FAILED_BOOKING_ALERT_WEBHOOK_URL is not configured. Skipping summary alert.")
        return False

    try:
        response = requests.post(webhook_url, json=summary_payload, timeout=30)
        response.raise_for_status()
        logging.info(
            "Failed booking summary alert sent successfully. batchId=%s, totalFailures=%s",
            summary_payload.get("batchId"),
            summary_payload.get("totalFailures")
        )
        return True
    except Exception as alert_ex:
        logging.error("Failed to send failed booking summary alert: %s", str(alert_ex))
        return False


def process_pending_failed_booking_summary_alerts() -> dict:
    """
    Finds AI-reviewed failed bookings that are waiting for notification,
    sends one summary alert, and marks the batch as notified.
    """
    now = utc_now()
    batch_limit = int(os.environ.get("FAILED_BOOKING_ALERT_BATCH_LIMIT", "100"))

    client = pymongo.MongoClient(
        os.environ["CosmosDBConnection"],
        tls=True
    )

    db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
    failed_col = db["FailedBookings"]

    query = {
        "reviewStatus": "PENDING",
        "notificationStatus": "PENDING",
        "aiReviewStatus": {"$in": ["COMPLETED", "FAILED"]}
    }

    failed_docs = list(
        failed_col.find(query).sort("createdAt", 1).limit(batch_limit)
    )

    if not failed_docs:
        logging.info("No pending failed booking summary alerts to send.")
        return {"message": "No pending failed booking summary alerts", "totalFailures": 0}

    oldest_created = failed_docs[0].get("createdAt")
    window_start = oldest_created if isinstance(oldest_created, datetime) else now - timedelta(minutes=10)
    batch_id = "FAILED_SUMMARY_" + now.strftime("%Y%m%dT%H%M%S%fZ")

    summary_payload = build_failed_booking_summary_payload(
        failed_docs=failed_docs,
        batch_id=batch_id,
        window_start=window_start,
        window_end=now
    )

    alert_sent = send_failed_booking_summary_alert(summary_payload)

    ids = [doc["_id"] for doc in failed_docs]
    update_doc = {
        "notificationStatus": "SENT" if alert_sent else "SKIPPED_OR_FAILED",
        "notified": bool(alert_sent),
        "notificationBatchId": batch_id,
        "notificationBatchSize": len(failed_docs),
        "notificationAttemptedAt": now,
        "updatedAt": now
    }

    if alert_sent:
        update_doc["notifiedAt"] = now
    else:
        update_doc["notificationError"] = "Summary alert failed. Check Logic App URL, Gmail connector, and Function logs."

    failed_col.update_many(
        {"_id": {"$in": ids}},
        {"$set": update_doc}
    )

    return {
        "batchId": batch_id,
        "totalFailures": len(failed_docs),
        "alertSent": alert_sent,
        "summaryPayload": summary_payload
    }


# =========================
# TEST AI AGENT API
# =========================
@app.function_name(name="test_ai_agent")
@app.route(
    route="test-ai-agent",
    methods=["GET", "POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def test_ai_agent(req: func.HttpRequest) -> func.HttpResponse:
    try:
        test_booking = {
            "bookingId": "BT-2026-0001",
            "origin": "Hyderabad",
            "destination": "Hyderabad",
            "currentProcessingYear": utc_now().year,
            "validationErrors": [
                "Invalid bookingId format",
                "Origin and destination cannot be same"
            ]
        }

        clean_ai_result = call_ai_review_model(test_booking)

        return func.HttpResponse(
            json.dumps(clean_ai_result, default=str),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error("TEST AI AGENT ERROR: %s", str(e))
        return func.HttpResponse(
            json.dumps({
                "error": "AI test failed",
                "details": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )


# =========================
# AI REVIEW FAILED BOOKING API
# =========================
@app.function_name(name="ai_review_failed_booking")
@app.route(
    route="ai-review-failed-booking",
    methods=["POST"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def ai_review_failed_booking(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
        booking_id = body.get("bookingId")

        if not booking_id:
            return func.HttpResponse(
                json.dumps({"error": "bookingId is required"}),
                status_code=400,
                mimetype="application/json"
            )

        booking_id = booking_id.strip().upper()

        client = pymongo.MongoClient(
            os.environ["CosmosDBConnection"],
            tls=True
        )

        db = client[os.environ.get("COSMOS_DB_NAME", "CorporateTravelDB")]
        failed_col = db["FailedBookings"]

        failed_booking = failed_col.find_one({
            "$or": [
                {"bookingId": booking_id},
                {"_id": booking_id}
            ]
        })

        if not failed_booking:
            return func.HttpResponse(
                json.dumps({"error": "Failed booking not found"}),
                status_code=404,
                mimetype="application/json"
            )

        booking_payload = build_ai_review_payload_from_failed_doc(failed_booking)
        clean_ai_result = call_ai_review_model(booking_payload)
        ai_review_doc = build_ai_review_doc(clean_ai_result)

        failed_col.update_one(
            {"_id": failed_booking["_id"]},
            {"$set": {
                "aiReview": ai_review_doc,
                "aiReviewStatus": "COMPLETED",
                "validationErrors": booking_payload.get("validationErrors", []),
                "notificationStatus": "PENDING",
                "notified": False,
                "updatedAt": utc_now()
            }}
        )

        return func.HttpResponse(
            json.dumps({
                "bookingId": booking_id,
                "bookingPayloadSentToAI": booking_payload,
                "aiReview": ai_review_doc
            }, default=str),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error("AI REVIEW FAILED BOOKING ERROR: %s", str(e))
        return func.HttpResponse(
            json.dumps({
                "error": "AI review failed",
                "details": str(e)
            }),
            status_code=500,
            mimetype="application/json"
        )

# =========================
# BATCHED FAILED BOOKING SUMMARY ALERT TIMER
# =========================
@app.function_name(name="failed_booking_summary_alert_timer")
@app.timer_trigger(
    schedule=os.environ.get("FAILED_BOOKING_SUMMARY_CRON", "0 */10 * * * *"),
    arg_name="timer",
    run_on_startup=False,
    use_monitor=True
)
def failed_booking_summary_alert_timer(timer: func.TimerRequest):
    """
    Sends one summarized email every 10 minutes by default.

    This prevents alert flooding when many bookings fail together.
    Default CRON: 0 */10 * * * *  -> every 10 minutes.
    """
    try:
        result = process_pending_failed_booking_summary_alerts()
        logging.info("Summary alert timer result: %s", json.dumps(result, default=str))
    except Exception as e:
        logging.error("FAILED BOOKING SUMMARY TIMER ERROR: %s", str(e))


# =========================
# MANUAL TEST API FOR SUMMARY ALERT
# =========================
@app.function_name(name="send_failed_booking_summary_now")
@app.route(
    route="send-failed-summary-now",
    methods=["POST", "GET"],
    auth_level=func.AuthLevel.ANONYMOUS
)
def send_failed_booking_summary_now(req: func.HttpRequest) -> func.HttpResponse:
    """Manual trigger to test the batched summary email without waiting for the timer."""
    try:
        result = process_pending_failed_booking_summary_alerts()
        return func.HttpResponse(
            json.dumps(result, default=str),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error("SEND FAILED SUMMARY NOW ERROR: %s", str(e))
        return func.HttpResponse(
            json.dumps({"error": "Failed to send summary alert", "details": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

