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
