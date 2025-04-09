# AI Voice Interviewer v1

A real-time, voice-based interview system powered by OpenAI's Realtime API and Twilio. This project simulates an AI interviewer hyping up a candidate (e.g., Ethan) for an internship, asking about background, skills, and vibes. It uses WebSockets for audio streaming, FastAPI for the server, and Twilio for phone integration.

## Features
- **Real-Time Voice Chat**: Streams audio via WebSockets with OpenAI’s `gpt-4o-realtime-preview-2024-10-01` model.
- **Phone Integration**: Connects through Twilio for real calls—say something and hear the AI respond!
- **Custom Interview**: Kicks off with a chill greeting ("Yoo im here to interview you Ethan, you ready???") and flows with tailored q’s.
- **Audio Format**: Rocks `g711_ulaw` (mu-law) for Twilio compatibility—8kHz audio in and out.
- **FastAPI Backend**: Runs a tight server for handling calls and streams.

## Prerequisites
- Python 3.13.2 or higher
- OpenAI API key with Realtime API access (`OPENAI_API_KEY` in `.env`)
- Twilio account with a phone number and credentials
- Ngrok for tunneling (get it from [ngrok.com](https://ngrok.com))
- Dependencies in `requirements.txt`

## Installation
1. **Clone the Repo**:
   ```bash
   git clone https://github.com/yourusername/ai-voice-interviewer-v1.git
   cd ai-voice-interviewer-v1