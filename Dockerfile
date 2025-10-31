FROM nvidia/cuda:12.1.1-runtime-ubuntu22.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY step-7-transcribe-mp3.py /app/step-7-transcribe-mp3.py

# GPU-enabled ctranslate2 (CUDA 12.1 wheels), then faster-whisper + boto3
RUN pip3 install --no-cache-dir "ctranslate2>=4.3.1" -f https://opennmt.net/ctranslate2/wheels/cu121/ \
 && pip3 install --no-cache-dir faster-whisper==1.0.3 boto3

ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/step-7-transcribe-mp3.py"]

