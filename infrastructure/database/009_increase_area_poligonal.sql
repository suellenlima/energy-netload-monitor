UPDATE transformadores SET area_poligonal_km = 2.0 WHERE area_poligonal_km <= 1.0;
SELECT MIN(area_poligonal_km), MAX(area_poligonal_km) FROM transformadores;
