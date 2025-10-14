curl -X POST "https://api.cloud.wherobots.com/runs?region=REGION" \
  -H "accept: application/json" \
  -H "X-API-Key: API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "runtime": "small",
    "name": "bronze_correct_geoms",
    "runPython": {
      "uri": "PATH"
    },
    "timeoutSeconds": 3600
  }'

