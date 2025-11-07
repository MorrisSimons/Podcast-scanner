import argparse
import os
from pathlib import Path
from typing import Iterable

from faster_whisper import WhisperModel

DEFAULT_AUDIO_DIR = "/Users/morrissimons/Desktop/Podcast scanner/output-mp3"


def collect_audio_files(directory: Path) -> Iterable[Path]:
    if not directory.exists():
        raise FileNotFoundError(f"Audio directory does not exist: {directory}")
    if not directory.is_dir():
        raise NotADirectoryError(f"Audio directory is not a directory: {directory}")

    files = sorted(path for path in directory.iterdir() if path.suffix.lower() == ".mp3")
    if not files:
        raise FileNotFoundError(f"No supported audio files found in {directory}")
    return files


def load_model(model_id: str, device: str, compute_type: str, download_root: Path) -> WhisperModel:
    return WhisperModel(
        model_id,
        device=device,
        compute_type=compute_type,
        download_root=str(download_root),
    )


def transcribe_with_timestamps(model: WhisperModel, audio_path: Path, language: str) -> None:
    segments, info = model.transcribe(
        str(audio_path),
        language=language,
        task="transcribe",
        vad_filter=True,
        temperature=0.0,
        condition_on_previous_text=False,
    )

    output_path = audio_path.with_suffix(".timestamps.txt")
    with output_path.open("w", encoding="utf-8") as handle:
        if getattr(info, "language", None):
            handle.write(f"# Detected language: {info.language} (p={getattr(info, 'language_probability', None)})\n")
        for segment in segments:
            start = segment.start
            end = segment.end
            text = segment.text.strip()
            handle.write(f"[{start:.2f} -> {end:.2f}] {text}\n")


def transcribe_without_timestamps(model: WhisperModel, audio_path: Path, language: str) -> None:
    segments, info = model.transcribe(
        str(audio_path),
        language=language,
        task="transcribe",
        vad_filter=True,
        temperature=0.0,
        condition_on_previous_text=False,
    )

    output_path = audio_path.with_suffix(".txt")
    with output_path.open("w", encoding="utf-8") as handle:
        if getattr(info, "language", None):
            handle.write(f"# Detected language: {info.language} (p={getattr(info, 'language_probability', None)})\n")
        for segment in segments:
            text = segment.text.strip()
            handle.write(f"{text}\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Transcribe local audio files with timestamps using Faster Whisper.")
    parser.add_argument(
        "--audio-dir",
        type=str,
        default=os.getenv("LOCAL_AUDIO_DIR", DEFAULT_AUDIO_DIR),
        help="Directory containing audio files to transcribe.",
    )
    parser.add_argument(
        "--model-id",
        type=str,
        default=os.getenv("WHISPER_MODEL_ID", "tiny"),
        help="Model identifier to load with Faster Whisper.",
    )
    parser.add_argument(
        "--device",
        type=str,
        default=os.getenv("WHISPER_DEVICE", "auto"),
        help="Device to run inference on (auto, cuda, cpu).",
    )
    parser.add_argument(
        "--compute-type",
        type=str,
        default=os.getenv("COMPUTE_TYPE", "auto"),
        help="Compute precision for Faster Whisper.",
    )
    parser.add_argument(
        "--language",
        type=str,
        default=os.getenv("TRANSCRIBE_LANGUAGE", "sv"),
        help="Language hint for transcription.",
    )
    parser.add_argument(
        "--cache-dir",
        type=str,
        default=os.getenv("CACHE_DIR", "cache"),
        help="Directory to store downloaded model files.",
    )
    parser.add_argument(
        "--no-timestamps",
        action="store_true",
        help="Output transcription without timestamps.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audio_dir = Path(args.audio_dir).expanduser().resolve()
    cache_dir = Path(args.cache_dir).expanduser().resolve()
    cache_dir.mkdir(parents=True, exist_ok=True)

    audio_files = collect_audio_files(audio_dir)
    model = load_model(args.model_id, args.device, args.compute_type, cache_dir)

    for audio_path in audio_files:
        print(f"Transcribing {audio_path}...")
        if args.no_timestamps:
            transcribe_without_timestamps(model, audio_path, args.language)
            print(f"Wrote {audio_path.with_suffix('.txt')}")
        else:
            transcribe_with_timestamps(model, audio_path, args.language)
            print(f"Wrote {audio_path.with_suffix('.timestamps.txt')}")


if __name__ == "__main__":
    main()
