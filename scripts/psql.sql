DELETE FROM usoil_data
WHERE datetime IN (
  SELECT datetime
  FROM usoil_data
  ORDER BY datetime DESC
  LIMIT 10
);

SELECT * FROM usoil_data ORDER BY datetime DESC LIMIT 10;