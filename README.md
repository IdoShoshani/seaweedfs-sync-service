# SeaweedFS Sync Service

A containerized Python service that monitors a local directory for new files and uploads them to a SeaweedFS cluster via the Filer API. The project includes Docker Compose for the SeaweedFS master/volume/filer components plus a host-side helper script that periodically generates files in the watched directory.

## Project Structure

```
.
├── client/                # Python watcher service (Docker image)
│   ├── app.py             # Watches for new files, uploads to SeaweedFS, logs storage stats
│   ├── Dockerfile         # Builds the client container image
│   └── requirements.txt   # Python dependencies for the client
├── docker-compose.yaml    # SeaweedFS cluster + Python client orchestration
├── scripts/
│   └── random_file_creator.py  # Host-side script that writes random files every 30–60 seconds
└── watched/               # Host directory mounted into the client container for monitoring
```

## Prerequisites

- Linux/Ubuntu host
- Docker and Docker Compose installed
- Python 3.10+ on the host (for running the host-side file generator script)

## Quick Start

1. **Clone the repository**

   ```bash
   git clone <repo-url>
   cd seaweedfs-sync-service
   ```

2. **Start the SeaweedFS cluster and watcher service**

   ```bash
   docker compose up --build
   ```

   This launches:

   - SeaweedFS master (`9333`), volume server (`8080`), and filer (`8888`)
   - The Python watcher service (`client/app.py`) which monitors `./watched` inside the container at `/app/watched`

3. **Generate files from the host (separate terminal)**

   ```bash
   python scripts/random_file_creator.py
   ```

   The script creates random text files in `./watched` every 30–60 seconds. The client container detects each file and uploads it to SeaweedFS via the Filer API to the root directory, then logs the updated storage usage from the master node.

4. **Confirm uploads**

   - Watch the Docker Compose logs for messages like `Successfully uploaded: <filename>` with file size and fid details.
   - Access the SeaweedFS filer UI at http://localhost:8888 to browse uploaded files in the root directory.
     ![Alt text](docs/images/filer.png?raw=true "filer screenshot")

5. **Stop the stack**
   ```bash
   docker compose down
   ```

## Service Details

### Client Upload Flow

- **File monitoring:** Implemented with `watchdog`; ignores hidden/temp/backup files and waits for file writes to stabilize before uploading.
- **Upload via Filer:** Sends files directly to SeaweedFS Filer root directory (`POST /uploads`), which automatically handles file ID assignment and storage distribution.
- **Filename preservation:** Original filenames are preserved in the distributed storage.
- **Storage reporting:** Queries `/vol/status` from the master node to log total bytes, file count, and volume count.
- **Reliability guards:** Tracks processing files to avoid duplicates, hashes content to prevent re-uploading identical files, and verifies both Filer and Master availability on startup.

### Health Checks on Startup

The service performs dual health checks:

1. **Filer readiness** (primary): Waits for Filer to respond at `http://filer:8888/`
2. **Master verification** (secondary): Confirms Master connectivity for storage statistics

## Configuration Notes

- The watched directory on the host is `./watched` (mounted into the client container at `/app/watched`). Ensure it is writable.
- Network ports exposed to the host:
  - Master: `9333`
  - Volume: `8080`
  - Filer: `8888`
- Service endpoints in `client/app.py`:
  - `FILER_URL = "http://filer:8888"` — Primary upload endpoint
  - `MASTER_URL = "http://master:9333"` — Storage statistics and cluster info
- Files are uploaded to the **root directory** of the SeaweedFS Filer (no subdirectories).
- To adjust the host file generation cadence, edit `MIN_DELAY` and `MAX_DELAY` in `scripts/random_file_creator.py`.

## Logs and Troubleshooting

- View runtime logs from all services:
  ```bash
  docker compose logs -f
  ```
- If the client exits because SeaweedFS Filer is unreachable, confirm the filer container is healthy and restart the stack.
- For Master connectivity warnings, verify that the Master service is running. The client can continue if Filer is available but will have limited storage reporting.
- The client uses standard output logging; adjust log level in `client/app.py` via `logging.basicConfig` if needed.

## Cleaning Up

- Remove containers and network:
  ```bash
  docker compose down
  ```
- Remove SeaweedFS data volume:
  ```bash
  docker volume rm seaweedfs-sync-service_seaweed_volume_data
  ```
