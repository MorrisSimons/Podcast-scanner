FROM nvidia/cuda:12.4.1-cudnn-runtime-ubuntu22.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    ffmpeg \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY step-7-transcribe-mp3.py /app/step-7-transcribe-mp3.py

# CUDA 12.4 wheels from OpenNMT
RUN pip3 install --no-cache-dir "ctranslate2>=4.5.0" -f https://opennmt.net/ctranslate2/wheels/cu124/ \
 && pip3 install --no-cache-dir faster-whisper==1.0.3 boto3 requests python-dotenv tqdm

ENV PYTHONUNBUFFERED=1
CMD ["python3", "/app/step-7-transcribe-mp3.py"]
