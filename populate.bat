ogr2ogr -f GeoJSON 263-ida.json 263-ida.kml

ogr2ogr -f "PostgreSQL" PG:"host=localhost user=ufg dbname=highway password=ufgufg" 263-volta.kml -nln linha263

ogr2ogr -f "PostgreSQL" PG:"host=localhost user=ufg dbname=highway password=ufgufg" 650-ida.kml -nln linha650


ogr2ogr -f "PostgreSQL" PG:"host=localhost user=ufg dbname=highway password=ufgufg" 400.kml -nln linha400