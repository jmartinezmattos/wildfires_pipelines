import re
import os
import csv
import torch
import shutil
import argparse
import numpy as np
from PIL import Image
from tqdm import tqdm
from datetime import datetime
from transformers import AutoModelForImageClassification, AutoImageProcessor


# =========================
# CONFIGURACIÓN
# =========================

DATA_DIR = "./data"
os.makedirs(DATA_DIR, exist_ok=True)

MODEL_PATH = "./models/efficientnet"

CSV_PATH = f"{DATA_DIR}/predictions_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
CSV_FIRE_PATH = f"{DATA_DIR}/predictions_fire_only_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
OUTPUT_FIRE_IMAGES_DIR = f"{DATA_DIR}/predictions_fire_images_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

DEVICE = "cuda" if torch.cuda.is_available() else "cpu"

BATCH_SIZE = 8 if DEVICE == "cpu" else 32

FIELDNAMES = [
    "filename",
    "prediction",
    "confidence",
    "prob_fire",
    "prob_no_fire",
]

parser = argparse.ArgumentParser(description="Run fire classification on images.")
parser.add_argument(
    "--images_dir",
    type=str,
    default=f"{DATA_DIR}/uruguay_tiles",
    help="Path to the directory containing images to process (default: ./data/uruguay_tiles)"
)
args = parser.parse_args()

IMAGES_DIR = args.images_dir

# =========================
# UTILIDADES
# =========================

def init_csv(path):
    if not os.path.exists(path):
        with open(path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
            writer.writeheader()


def extract_number(s):
    m = re.search(r"(\d+)", s)
    return int(m.group(1)) if m else float("inf")


def chunks(lst, n):
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def inference(images_dir=IMAGES_DIR):
    # =========================
    # INICIALIZACIÓN
    # =========================

    init_csv(CSV_PATH)

    print("Loading model...")
    model = AutoModelForImageClassification.from_pretrained(MODEL_PATH)
    processor = AutoImageProcessor.from_pretrained(MODEL_PATH)

    model.to(DEVICE)
    model.eval()

    # Limitar threads (mejora estabilidad en CPU)
    torch.set_num_threads(max(1, os.cpu_count() // 2))

    id2label = model.config.id2label
    label2id = model.config.label2id

    print("Labels:", id2label)

    # =========================
    # LISTADO Y ORDEN DE IMÁGENES
    # =========================

    image_files = [
        f for f in os.listdir(images_dir)
        if f.lower().endswith((".jpg", ".jpeg", ".png"))
    ]

    image_files = sorted(image_files, key=lambda x: (extract_number(x), x))

    print(f"Found {len(image_files)} images (sorted numerically)")


    # =========================
    # REANUDACIÓN (opcional)
    # =========================

    processed = set()
    if os.path.exists(CSV_PATH):
        with open(CSV_PATH, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                processed.add(row["filename"])

    if processed:
        image_files = [f for f in image_files if f not in processed]
        print(f"Resuming: {len(image_files)} images remaining")


    # =========================
    # INFERENCIA + CSV INCREMENTAL
    # =========================

    csv_file = open(CSV_PATH, mode="a", newline="", encoding="utf-8")
    writer = csv.DictWriter(csv_file, fieldnames=FIELDNAMES)

    for batch_files in tqdm(list(chunks(image_files, BATCH_SIZE))):
        images = []
        valid_fnames = []

        for fname in batch_files:
            img_path = os.path.join(images_dir, fname)
            try:
                with Image.open(img_path) as img:
                    images.append(img.convert("RGB"))
                    valid_fnames.append(fname)
            except Exception as e:
                print(f"Skipping {fname}: {e}")

        if not images:
            continue

        inputs = processor(images=images, return_tensors="pt")
        inputs = {k: v.to(DEVICE) for k, v in inputs.items()}

        with torch.no_grad():
            outputs = model(**inputs)
            probs = torch.softmax(outputs.logits, dim=-1).cpu().numpy()

        for fname, p in zip(valid_fnames, probs):
            pred_idx = int(np.argmax(p))
            pred_label = id2label[pred_idx]

            writer.writerow({
                "filename": fname,
                "prediction": pred_label,
                "confidence": float(p[pred_idx]),
                "prob_fire": float(p[label2id["Fire"]]),
                "prob_no_fire": float(p[label2id["No_Fire"]]),
            })

        # Persistir resultados aunque el proceso se caiga
        csv_file.flush()

    csv_file.close()

    print("Generating Fire-only CSV...")

    fire_rows = []

    with open(CSV_PATH, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["prediction"] == "Fire":
                fire_rows.append(row)

    with open(CSV_FIRE_PATH, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        for row in fire_rows:
            writer.writerow(row)

    print(f"Fire-only CSV written: {CSV_FIRE_PATH}")
    print(f"Total Fire detections: {len(fire_rows)}")

    print("Copying Fire images to:", OUTPUT_FIRE_IMAGES_DIR)

    os.makedirs(OUTPUT_FIRE_IMAGES_DIR, exist_ok=True)

    for row in fire_rows:
        src_path = os.path.join(images_dir, row["filename"])
        dst_path = os.path.join(OUTPUT_FIRE_IMAGES_DIR, row["filename"])
        try:
            shutil.copy2(src_path, dst_path)
        except Exception as e:
            print(f"Failed to copy {row['filename']}: {e}")

    print("Fire images copied successfully.")
    print("All done.")

    return OUTPUT_FIRE_IMAGES_DIR

if __name__ == "__main__":
    
    inference()
