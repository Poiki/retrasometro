# Railway reference geometry

`railways.geojson` is an OpenStreetMap snapshot of ways tagged `railway=rail` in the Iberian Peninsula, the Balearic Islands and the surrounding border area (35–44.5° N, 10° W–4.5° E). Each way retains its track geometry, rounded to five decimal places; no train GPS coordinates are used to invent track lines.

Data © [OpenStreetMap contributors](https://www.openstreetmap.org/copyright), available under the [Open Data Commons Open Database License 1.0](https://opendatacommons.org/licenses/odbl/1-0/). This data license is separate from the application source license. The GeoJSON properties retain the upstream snapshot timestamp, attribution and extraction query.

Refresh manually from the repository root:

```sh
python3 scripts/update-railways.py
```

The updater requires only Python's standard library. It validates a complete Overpass response before atomically replacing the snapshot. Restart the service after refreshing so that the analytics worker reloads the geometry. The application does not query Overpass for each page visit or ingestion cycle.

The map highlights only reference track portions inside detected areas. It does not assign a reported slowdown to a particular parallel track. Newly built or altered lines require a refreshed snapshot.
