from supabase import create_client, Client
import uuid  # for generating a new call_logs.id if needed
from datetime import datetime
import os
import json
import base64
import asyncio
import websockets
from fastapi import FastAPI, WebSocket, Request
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.websockets import WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from twilio.twiml.voice_response import VoiceResponse, Connect
from twilio.rest import Client
from pydantic import BaseModel, Field # Import Pydantic

from dotenv import load_dotenv

# Debug imports
import sys
print(f"Python version: {sys.version}")
print(f"websockets path: {websockets.__file__}")
print(f"websockets version: {websockets.__version__}")

# Load the .env file
load_dotenv()

# Set up constants
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PUBLIC_SERVER_URL = os.getenv('PUBLIC_SERVER_URL')
PORT = int(os.getenv('PORT', 8080))
SYSTEM_MESSAGE = (
    "Hey Ethan, I'm your AI interviewer—great to connect with you! "
    "Before we dive into internship details, how's your day been so far? Feel free to take a moment to gather your thoughts. "
    "Here's our plan: "
    "First, tell me your story—what inspired you to pursue this field and get into this role? "
    "Next, share the skills and experiences you bring to our team. Take your time. "
    "Finally, let's talk about what excites you most about this internship opportunity. "
    "I'll follow up on your answers, ensuring we stay focused on the internship. If you stray off-topic, I'll prompt you with, 'Nice, how does that relate to the internship?' "
    "Remember to speak slowly and clearly—I'm here to make this a comfortable and engaging conversation. Let's get started!"
)
VOICE = 'coral'
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_PHONE_NUMBER = os.getenv("TWILIO_PHONE_NUMBER")
client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)


# Retrieve Supabase URL and Key from your environment
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Create the Supabase client instance
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# Initialize FastAPI application instance
app = FastAPI()

# Add CORS middleware
# TODO: Restrict origins for production
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # Allows all origins
    allow_credentials=True,
    allow_methods=["*"], # Allows all methods
    allow_headers=["*"], # Allows all headers
)

# Check for OpenAI API key
if not OPENAI_API_KEY:
    raise ValueError('MISSING OPENAI API KEY!')
if not PUBLIC_SERVER_URL:
    raise ValueError('MISSING PUBLIC_SERVER_URL environment variable!')

# --- Pydantic Models ---
class MakeCallRequest(BaseModel):
    candidate_id: uuid.UUID = Field(..., description="The UUID of the candidate to call")

# --- API Endpoints ---

@app.get("/", response_class=JSONResponse)
async def index_page():
    return {"message": "Twilio AI Interviewer is working!"}


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    response = VoiceResponse()
    websocket_url = f"{PUBLIC_SERVER_URL.replace('https', 'wss')}/media-stream"
    print(f"Connecting Twilio Stream to: {websocket_url}")
    connect = Connect()
    connect.stream(url=websocket_url)
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")


# Handles the outbound call based on candidate_id
@app.post("/make-call")
async def make_outbound_call(call_request: MakeCallRequest): # Accept request body
    candidate_id = call_request.candidate_id
    print(f"Attempting to make call to candidate_id: {candidate_id}")
    try:
        # ✅ Step 1: Retrieve the specified candidate record from Supabase.
        response = supabase.table("candidates")\
            .select("id, phone_number")\
            .eq("id", str(candidate_id))\
            .limit(1)\
            .execute()

        # Check if a candidate record was found.
        candidate = response.data[0] if response.data else None
        if not candidate:
            print(f"Candidate with ID {candidate_id} not found in Supabase.")
            return JSONResponse({"error": f"Candidate with ID {candidate_id} not found"}, status_code=404)

        # Extract the candidate's phone number (ID is already known)
        phone_number = candidate["phone_number"]
        if not phone_number:
            print(f"Candidate {candidate_id} found but has no phone number.")
            return JSONResponse({"error": f"Candidate {candidate_id} has no phone number"}, status_code=400)

        # ✅ Step 2: Use Twilio to initiate an outbound call to the candidate's phone number.
        twilio_callback_url = f"{PUBLIC_SERVER_URL}/incoming-call"
        print(f"Setting Twilio callback URL to: {twilio_callback_url}")
        call = client.calls.create(
            to=phone_number,
            from_=TWILIO_PHONE_NUMBER,
            url=twilio_callback_url
        )

        # ✅ Step 3: Log the call in the call_logs table in Supabase.
        # Use the provided candidate_id
        supabase.table("call_logs").insert({
            "id": str(uuid.uuid4()),
            "candidate_id": str(candidate_id), # Use the provided ID
            "status": "initiated",
            "started_at": datetime.now().isoformat() # Keep user change
        }).execute()

        # ✅ Step 4: Return a JSON response confirming the call and showing key details.
        return JSONResponse({
            "status": "calling",
            "candidate_id": str(candidate_id), # Return the ID that was called
            "call_sid": call.sid
        })

    except Exception as e:
        print(f"Error during make_outbound_call for candidate {candidate_id}: {e}")
        # ❌ If any errors occur, catch them and return a clean error message.
        return JSONResponse({"error": str(e)}, status_code=500)


@app.websocket("/media-stream")
async def handle_media_stream(websocket: WebSocket):
    print("Client connected")
    await websocket.accept()

    url = 'wss://api.openai.com/v1/realtime?model=gpt-4o-realtime-preview-2024-10-01'
    headers = [
        ("Authorization", f"Bearer {OPENAI_API_KEY}"),
        ("OpenAI-Beta", "realtime=v1")
    ]

    try:
        async with websockets.connect(url, extra_headers=headers) as openai_ws:
            print("Connected to OpenAI WebSocket")
            await send_session_update(openai_ws)
            stream_sid = None

            async def receive_from_twilio():
                nonlocal stream_sid
                try:
                    async for message in websocket.iter_text():
                        data = json.loads(message)
                        print(f"Twilio event: {data['event']}")
                        if data['event'] == 'start':
                            stream_sid = data['start']['streamSid']
                            print(f"Incoming stream started: {stream_sid}")
                        elif data['event'] == 'media' and openai_ws.open:
                            print(
                                f"Sending audio to OpenAI: {len(data['media']['payload'])} bytes")
                            audio_append = {
                                "type": "input_audio_buffer.append",
                                "audio": data['media']['payload']
                            }
                            await openai_ws.send(json.dumps(audio_append))
                except WebSocketDisconnect:
                    print("Twilio WebSocket disconnected.")
                    if openai_ws.open:
                        await openai_ws.close()

            async def send_to_twilio():
                nonlocal stream_sid
                try:
                    async for openai_message in openai_ws:
                        response = json.loads(openai_message)
                        print(f"OpenAI event: {response['type']}")
                        if response['type'] == 'session.updated':
                            print("Session updated successfully:", response)
                        elif response['type'] == 'response.audio.delta' and response.get('delta'):
                            audio_payload = base64.b64encode(
                                base64.b64decode(response['delta'])).decode('utf-8')
                            audio_delta = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": audio_payload}
                            }
                            await websocket.send_json(audio_delta)
                except websockets.ConnectionClosed as e:
                    print(f"OpenAI WebSocket closed: {e}")
                except Exception as e:
                    print(f"Error in send_to_twilio: {e}")

            await asyncio.gather(receive_from_twilio(), send_to_twilio())
    except Exception as e:
        print(f"Failed to connect to OpenAI: {e}")


async def send_session_update(openai_ws):
    session_update = {
        "type": "session.update",
        "session": {
            "turn_detection": {"type": "server_vad", "silence_duration_ms": 300},
            "input_audio_format": "g711_ulaw",
            "output_audio_format": "g711_ulaw",
            "voice": VOICE,
            "instructions": SYSTEM_MESSAGE,
            "modalities": ["text", "audio"],
            "temperature": 0.7,
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
