import gdown
import pandas as pd
import os
import shutil
import time
import logging
from sqlalchemy import create_engine

# =====================================
# CONFIG
# =====================================

CHUNK_SIZE = 100000  # rows per chunk

# =====================================
# CREATE LOG FOLDER
# =====================================

os.makedirs("logs", exist_ok=True)

# =====================================
# LOGGING CONFIG
# =====================================

logging.basicConfig(
    filename='logs/ingestion_db.log',
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    filemode="a"
)

# =====================================
# DATABASE CONNECTION
# =====================================

engine = create_engine("sqlite:///inventory.db")


# =====================================
# HELPER FUNCTION
# =====================================

def print_status(message, status="INFO"):
    line = f"[{status}] {message}"
    print(line)
    logging.info(line)


# =====================================
# DOWNLOAD DATASET
# =====================================

def download_dataset():

    start = time.time()

    folder_url = "https://drive.google.com/drive/folders/1NEPUQIMhbquKzjcD40R4aKxyo2nv7QhW?usp=sharing"
    output_dir = "my_dataset_folder"

    print_status("Starting dataset download...")

    try:

        if os.path.exists(output_dir):
            print_status("Removing old dataset folder...")
            shutil.rmtree(output_dir)

        gdown.download_folder(
            url=folder_url,
            output=output_dir,
            quiet=False
        )

        end = time.time()

        print_status(
            f"Dataset download completed in {(end-start)/60:.2f} minutes",
            "SUCCESS"
        )

    except Exception as e:

        logging.exception("Download failed")
        print(f"[ERROR] Download failed -> {e}")
        raise


# =====================================
# INGEST CHUNK INTO DATABASE
# =====================================

def ingest_chunk(chunk, table_name, if_exists_mode):

    try:

        chunk.to_sql(
            table_name,
            con=engine,
            if_exists=if_exists_mode,
            index=False
        )

    except Exception as e:

        logging.exception(f"Chunk ingestion failed for {table_name}")
        print(f"[ERROR] Chunk insertion failed -> {e}")
        raise


# =====================================
# PROCESS LARGE FILES IN CHUNKS
# =====================================

def process_large_csv(file_path, table_name):

    print_status(f"Chunk processing started for {table_name}")

    chunk_number = 1

    total_rows = 0

    start = time.time()

    try:

        # Read file in chunks
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):

            rows = len(chunk)

            total_rows += rows

            print("\n" + "-" * 50)

            print_status(
                f"Processing Chunk {chunk_number}"
            )

            print_status(
                f"Chunk Rows -> {rows}"
            )

            # First chunk replaces table
            if chunk_number == 1:
                mode = "replace"
            else:
                mode = "append"

            ingest_chunk(
                chunk=chunk,
                table_name=table_name,
                if_exists_mode=mode
            )

            print_status(
                f"Chunk {chunk_number} inserted successfully",
                "SUCCESS"
            )

            chunk_number += 1

        end = time.time()

        print("\n" + "=" * 60)

        print_status(
            f"{table_name} ingestion completed",
            "SUCCESS"
        )

        print_status(
            f"Total Rows Processed -> {total_rows}"
        )

        print_status(
            f"Total Chunks -> {chunk_number - 1}"
        )

        print_status(
            f"Time Taken -> {(end-start)/60:.2f} minutes"
        )

    except Exception as e:

        logging.exception(f"Large file processing failed: {table_name}")

        print(f"\n[ERROR] Failed processing {table_name}")
        print(f"[ERROR DETAILS] {e}")


# =====================================
# LOAD ALL FILES
# =====================================

def load_raw_data():

    start = time.time()

    folder = "my_dataset_folder"

    print_status("Starting ingestion process...")

    try:

        files = [file for file in os.listdir(folder) if file.endswith(".csv")]

        total_files = len(files)

        print_status(f"Total CSV files found -> {total_files}")

        if total_files == 0:
            print_status("No CSV files found", "WARNING")
            return

        for idx, file in enumerate(files, start=1):

            file_path = os.path.join(folder, file)

            table_name = file[:-4]

            print("\n" + "=" * 60)

            print_status(f"File {idx}/{total_files}")
            print_status(f"Current File -> {file}")

            try:

                # FILE SIZE
                file_size_gb = os.path.getsize(file_path) / (1024**3)

                print_status(
                    f"File Size -> {file_size_gb:.2f} GB"
                )

                # CHUNK PROCESSING
                process_large_csv(
                    file_path=file_path,
                    table_name=table_name
                )

            except Exception as e:

                logging.exception(f"Failed file -> {file}")

                print(f"\n[ERROR] Failed processing {file}")
                print(f"[ERROR DETAILS] {e}")

        end = time.time()

        print("\n" + "=" * 60)

        print_status(
            f"FULL INGESTION COMPLETED in {(end-start)/60:.2f} minutes",
            "SUCCESS"
        )

    except Exception as e:

        logging.exception("Pipeline failed")

        print(f"\n[CRITICAL ERROR] {e}")


# =====================================
# MAIN
# =====================================

if __name__ == '__main__':

    pipeline_start = time.time()

    print("\n" + "=" * 60)
    print("        LARGE SCALE ETL PIPELINE STARTED")
    print("=" * 60)

    try:

        download_dataset()

        load_raw_data()

        pipeline_end = time.time()

        print("\n" + "=" * 60)

        print_status(
            f"PIPELINE FINISHED in {(pipeline_end-pipeline_start)/60:.2f} minutes",
            "SUCCESS"
        )

        print("=" * 60)

    except Exception as e:

        logging.exception("Pipeline crashed")

        print(f"\n[FATAL ERROR] {e}")