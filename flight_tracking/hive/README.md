# Hive

This step will define the persistent storage tables for processed aircraft analytics.

Main file to implement:

```text
create_tables.sql
```

Run the Hive DDL:

```powershell
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -f /workspace/flight_tracking/hive/create_tables.sql
```

Check the country summary table:

```powershell
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT origin_country, aircraft_count, ROUND(avg_velocity_mps, 2) FROM flight_tracking.aircraft_country_summary ORDER BY aircraft_count DESC LIMIT 10;"
```

Check the enriched bonus table:

```powershell
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT origin_country, region, continent, aircraft_count FROM flight_tracking.aircraft_country_summary_enriched ORDER BY aircraft_count DESC LIMIT 10;"
```

Check the latest aircraft table:

```powershell
docker exec flight-hive beeline -u jdbc:hive2://localhost:10000 -e "SELECT callsign, origin_country, latitude, longitude, ROUND(velocity, 2) FROM flight_tracking.latest_aircraft_states ORDER BY velocity DESC LIMIT 10;"
```
