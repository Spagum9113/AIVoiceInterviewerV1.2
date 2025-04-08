""" Trying to measure latency with this script"""

import sys
from dotenv import load_dotenv
from twilio.twiml.voice_response import VoiceResponse, Connect
from fastapi.websockets import WebSocketDisconnect
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi import FastAPI, WebSocket, Request
import websockets
import asyncio
import base64
import json
import os
import time

# Debug imports
print(f"Python version: {sys.version}")
print(f"websockets path: {websockets.__file__}")
print(f"websockets version: {websockets.__version__}")

# Load the .env file
load_dotenv()

# Set up constants
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')
PORT = int(os.getenv('PORT', 5050))
VOICE = 'alloy'
SYSTEM_MESSAGE = """
Hi Ethan! I’m thrilled to be your interviewer today for this internship opportunity. My goal is to get to know you—your background, skills, and what excites you about this role. I’ll ask a variety of questions to understand how your experiences align with our team’s needs. Take your time with your answers, and feel free to ask me anything if you need clarification. Let’s make this a relaxed, open chat—ready to dive in?

Interview Guidelines:
1. Start with a warm, personalized greeting using the candidate’s name (e.g., 'Hi Ethan!').
2. Ask 5-7 thoughtful questions, beginning with an icebreaker (e.g., 'Tell me a bit about yourself'), then exploring skills (e.g., 'What’s a project you’re proud of and why?'), experience (e.g., 'Can you share a time you worked through a challenge with a team?'), and motivation (e.g., 'What draws you to this internship?').
3. Tailor follow-ups to the candidate’s responses for deeper insight (e.g., if coding comes up, ask, 'What’s your go-to programming language and why?').
4. Maintain a friendly, supportive, and professional tone—avoid anything critical or intimidating.
5. If the candidate hesitates, offer a gentle prompt (e.g., 'No rush—maybe start with an example that comes to mind').
6. Conclude with, 'Thanks so much for your time, Ethan! It’s been great learning about you, and we’ll follow up soon about next steps.'
7. Steer clear of judgments or definitive outcomes (e.g., don’t say ‘You’re a perfect fit!’ or ‘This might not work’).
"""

# Initialize FastAPI application instance
app = FastAPI()

# Check for OpenAI API key
if not OPENAI_API_KEY:
    raise ValueError('MISSING OPENAI API KEY!')


@app.get("/", response_class=JSONResponse)
async def index_page():
    return {"message": "Twilio is working!"}


@app.api_route("/incoming-call", methods=["GET", "POST"])
async def handle_incoming_call(request: Request):
    response = VoiceResponse()
    host = request.url.hostname
    connect = Connect()
    connect.stream(url=f"wss://{host}/media-stream")
    response.append(connect)
    return HTMLResponse(content=str(response), media_type="application/xml")


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

            last_speech_time = None  # When speech ends (set at speech_stopped)
            first_response_time = None  # When AI first replies

            async def receive_from_twilio():
                nonlocal stream_sid, last_speech_time
                try:
                    async for message in websocket.iter_text():
                        data = json.loads(message)
                        if data['event'] == 'start':
                            stream_sid = data['start']['streamSid']
                            print(f"Incoming stream started: {stream_sid}")
                        elif data['event'] == 'media' and openai_ws.open:
                            # Get timestamp for sending audio
                            current_time = time.time()

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
                nonlocal stream_sid, last_speech_time, first_response_time
                try:
                    async for openai_message in openai_ws:
                        response = json.loads(openai_message)
                        # Timestamp all OpenAI events
                        event_time = time.time()
                        print(
                            f"OpenAI event at {event_time:.3f} seconds: {response['type']}")

                        if response['type'] == 'input_audio_buffer.speech_stopped':
                            last_speech_time = time.time()
                            print(
                                f"Speech stopped at {last_speech_time:.3f} seconds")
                        elif response['type'] == 'session.updated':
                            print("Session updated successfully:", response)
                        elif response['type'] == 'response.audio.delta' and response.get('delta'):
                            if first_response_time is None:  # First chunk of AI response
                                first_response_time = time.time()
                                print(
                                    f"First response at {first_response_time:.3f} seconds")
                                if last_speech_time:
                                    latency = (first_response_time -
                                               last_speech_time) * 1000
                                    print(
                                        f"Latency from speech end to AI response: {latency:.2f} ms")
                            audio_payload = base64.b64encode(
                                base64.b64decode(response['delta'])).decode('utf-8')
                            audio_delta = {
                                "event": "media",
                                "streamSid": stream_sid,
                                "media": {"payload": audio_payload}
                            }
                            await websocket.send_json(audio_delta)
                            first_response_time = None
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
            "turn_detection": {"type": "server_vad"},
            "input_audio_format": "g711_ulaw",
            "output_audio_format": "g711_ulaw",
            "voice": VOICE,
            "instructions": SYSTEM_MESSAGE,
            "modalities": ["text", "audio"],
            "temperature": 0.8,
        }
    }
    print('Sending session update:', json.dumps(session_update))
    await openai_ws.send(json.dumps(session_update))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
