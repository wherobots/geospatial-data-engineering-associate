# Wherobots Geospatial Data Engineering Associate - Course Repo

Welcome to the **Wherobots Geospatial Data Engineering Associate** course!
This repository contains the code, data references, and weekly exercises we’ll use as we progress from raw geospatial datasets to a scalable, cloud-native data lake with **Apache Sedona**, **WherobotsDB**, and **Apache Iceberg**.

---

## Course Schedule

* **Tuesdays — Lesson & Code Review**
  Live session walking through new concepts and reviewing code.

* **Between Tuesday & Thursday — Work Time**
  Complete your weekly notebook and push progress.

* **Thursdays — Office Hours**
  Open Q&A and debugging help.

---

## Certification

To earn your certificate:

1. Complete each weekly exercise.
2. Export your final notebook as a `.md` file.
3. Submit at the end of each week (you’ll be prompted with the link).

---

## Week 1 — Setup & First Pipeline

### Goals

* Get your environment ready.
* Learn the **Bronze → Silver → Gold** pattern (multi-hop).
* Ingest your first geospatial dataset into a temporary **Sedona DataFrame**.

### Steps

1. **Clone this repo**

```bash
git clone https://github.com/wherobots/geospatial-data-engineering-associate.git
cd geospatial-data-engineering-associate
```

2. **Sign up for Wherobots**

* Create a free [Wherobots account](https://wherobots.com).
* Pro subscription is **not required yet** but will be needed for later notebooks.
* If upgrading, you can activate via [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-ndy62v6hhwrne?sr=0-1&ref_=beagle&applicationId=AWSMPContessa).

3. **Upload the first notebook**

Sign into your Wherobots account and upload the Week 1 notebook.

Open `week-1.ipynb` and follow the instructions to:

* Connect to a sample vector & raster dataset.
* Create a Sedona DataFrame.
* Inspect schema and geometry validity.

4. **Export & Submit**
   After completing the notebook:

* In the Jupyter notebook go to File -> Save and Export Notebook As -> PDF 

## Estimated Notebook Runtimes & Costs

Below is an estimated cost table for running the notebooks from end to end with no breaks or pauses. When runtimes are inactive Wherobots will scale down to the lowest level of compute needed to keep that notebook running however Spatial Units will still be consumed. 

We recommend reviewing the notebook and then running the commands step by step, and destroying the runtime when complete to maintain the lowest possible costs.

Estimates are based on running in US East or US West. Please refer to [Wherobots Pricing](https://wherobots.com/pricing/) for more details. These are estimates only using costs with full resource utilization and may not reflect actual costs incured which depends on the time notebooks are run and other factors.

- Tiny: ~65 minutes or $8.13
- Small: ~35 minutes or $7.88
- Medium: ~14 minutes or $17.50 

## Week 2 — Bonze Tables, Jobs, and Spatial SQL API Tools

Here’s the **expanded README** with your new **Week 2 instructions** added in a clean, consistent Markdown style that matches the rest of your repo:

---

# Wherobots Geospatial Data Engineering Associate - Course Repo

Welcome to the **Wherobots Geospatial Data Engineering Associate** course!
This repository contains the code, data references, and weekly exercises we’ll use as we progress from raw geospatial datasets to a scalable, cloud-native data lake with **Apache Sedona**, **WherobotsDB**, and **Apache Iceberg**.

---

## Course Schedule

* **Tuesdays — Lesson & Code Review**
  Live session walking through new concepts and reviewing code.

* **Between Tuesday & Thursday — Work Time**
  Complete your weekly notebook and push progress.

* **Thursdays — Office Hours**
  Open Q&A and debugging help.

---

## Certification

To earn your certificate:

1. Complete each weekly exercise.
2. Export your final notebook as a `.md` file.
3. Submit at the end of each week (you’ll be prompted with the link).

---

## Week 1 — Setup & First Pipeline

### Goals

* Get your environment ready.
* Learn the **Bronze → Silver → Gold** pattern (multi-hop).
* Ingest your first geospatial dataset into a temporary **Sedona DataFrame**.

### Steps

1. **Clone this repo**

   ```bash
   git clone https://github.com/wherobots/geospatial-data-engineering-associate.git
   cd geospatial-data-engineering-associate
   ```

2. **Sign up for Wherobots**

   * Create a free [Wherobots account](https://wherobots.com).
   * Pro subscription is **not required yet** but will be needed for later notebooks.
   * If upgrading, you can activate via [AWS Marketplace](https://aws.amazon.com/marketplace/pp/prodview-ndy62v6hhwrne?sr=0-1&ref_=beagle&applicationId=AWSMPContessa).

3. **Upload the first notebook**

   * Sign into your Wherobots account and upload the Week 1 notebook.
   * Open `week-1.ipynb` and follow the instructions to:

     * Connect to a sample vector & raster dataset.
     * Create a Sedona DataFrame.
     * Inspect schema and geometry validity.

4. **Export & Submit**

   * In the Jupyter notebook go to **File → Save and Export Notebook As → PDF**

---

## Estimated Notebook Runtimes & Costs

Below is an estimated cost table for running the notebooks from end to end with no breaks or pauses. When runtimes are inactive, Wherobots will scale down automatically, but Spatial Units are still consumed.

We recommend reviewing the notebook first, then running the commands step-by-step, and destroying the runtime when complete to minimize costs.

Estimates are based on running in **US East** or **US West**. See [Wherobots Pricing](https://wherobots.com/pricing/) for current details.

| Runtime | Approx Duration | Est. Cost (USD) |
| ------- | --------------- | --------------- |
| Tiny    | ~65 min         | $8.13           |
| Small   | ~35 min         | $7.88           |
| Medium  | ~14 min         | $17.50          |

---

## Week 2 — Bronze Tables, Jobs, and Spatial SQL API Tools

### Goals

* Learn to create Bronze-level tables using Wherobots Jobs.
* Perform geometry corrections and coordinate system standardization.
* Validate geometries and projections using Harlequin.

---

### Step 1 – Upload Your Python Files

1. Open **Wherobots Cloud → Storage**.
2. Click the **purple Upload** button and upload your `.py` files.
3. Make note of the full **S3 path** to your uploaded script (you’ll need it later).

> **Note:** For this week, you only need to run the file
> `week-2-geom-corrections-crs.py`.

---

### Step 2 – Create an API Key

1. Go to your **Account Settings** (bottom-left corner).
2. Navigate to **Settings → API Keys**.
3. Click **Create Key** and **store it securely** — treat it like a password.

---

### Step 3 – Run Your Job via the API

Run the following `curl` command in your terminal or command line:

```bash
curl -X POST "https://api.cloud.wherobots.com/runs?region=aws-us-east-1" \
  -H "accept: application/json" \
  -H "X-API-Key: <YOUR_API_KEY>" \
  -H "Content-Type: application/json" \
  -d '{
    "runtime": "tiny",
    "name": "bronze_correct_geoms",
    "runPython": {
      "uri": "<YOUR_FILE_PATH>"
    },
    "timeoutSeconds": 3600
  }'
```

Replace:

* `<YOUR_API_KEY>` → your actual Wherobots API key.
* `<YOUR_FILE_PATH>` → the S3 URI for your uploaded `.py` file.
* Optionally adjust `"region"` to match your Wherobots region (e.g., `aws-us-west-2`).

---

### Step 4 – Validate Results in Harlequin

Install and connect **Harlequin with the Wherobots adapter**:

```bash
pip install harlequin-wherobots
harlequin -a wherobots --api-key <YOUR_API_KEY> --runtime SEDONA --region AWS_US_WEST_2
```

Once connected, verify your table’s geometry integrity and projection.

**Check for invalid geometries:**

```sql
SELECT COUNT(*)
FROM org_catalog.gde_bronze.table_name
WHERE ST_IsValid(geometry) = FALSE;
```

**Check the coordinate reference system (CRS):**

```sql
SELECT ST_SRID(geometry)
FROM org_catalog.gde_bronze.table_name
LIMIT 10;
```

If all geometries are valid and SRIDs match your target projection (typically EPSG 4326), your Bronze stage data is ready for Week 3.

---

**Deliverable:**
Upload a short summary or screenshot confirming:

* Job ran successfully
* Invalid geometry count = 0
* SRID = 4326

Then submit your completion form for Week 2.

Estimates are based on running in **US East** or **US West** using maximum resources. Actual costs may vary. See [Wherobots Pricing](https://wherobots.com/pricing/) for current details.

| Runtime | Approx Duration | Est. Cost (USD) |
| ------- | --------------- | --------------- |
| Tiny    | ~6.5 min        | $0.82           |
| Small   | ~5.5 min        | $1.24           |
| Medium  | ~4.5 min        | $5.60           |