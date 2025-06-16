Cześć,

uwagi co do rozwiązania:
- jako źródła użyłem plików parquet, są w nich dane z wysłanego excel
- w kodzie dodałem kilka razy "show" żeby pokazać jak wyglądały dane pomiędzy warstwami/transformacjami
- co do uruchomienia kodu, w instrukcji nie był uścislony sposób dostarczenia rozwiązania - zrobiłem to z intelij odpalając lokalnie, tez w repo dorzuciłem pliki z przykładowymi danymi żeby można było to łatwo uruchomić i sprawdzić
- ale dodałem tez config gdzie można zmienić path do plików
- dodałem testy które można uruchomić
- jeżeli powinienem coś jeszcze dodać/zmienić to śmiało można pisać linkedin/mail

zauważyłem też kilka braków w wymaganiach:
- na jakiej podstawie wybierać które duplikaty usuwać przy tym samym ID i dacie
- co jeżeli suma gwarancji przekracza 100%
- i przy innych nieprawidłowych wartościach, datach itp.

+-----+-------+--------------+-----------+-----------+--------+-----------+-----------+-----------+-----------+-----------+
| NAME|COUNTRY|PARTITION_DATE|    CLASS_A|    CLASS_B| CLASS_C|    CLASS_D|AVG_CLASS_A|AVG_CLASS_B|AVG_CLASS_C|AVG_CLASS_D|
+-----+-------+--------------+-----------+-----------+--------+-----------+-----------+-----------+-----------+-----------+
|G_001|     ES|    25.01.2024| 600.000000|4000.000000|0.000000|   0.000000| 200.000000|3000.000000|   0.000000|   0.000000|
|G_002|     ES|    25.01.2024|   0.000000|1000.000000|0.000000|   0.000000| 200.000000|3000.000000|   0.000000|   0.000000|
|G_004|     ES|    25.01.2024|   0.000000|4000.000000|0.000000|   0.000000| 200.000000|3000.000000|   0.000000|   0.000000|
|G_005|     US|    25.01.2024|2000.000000|   0.000000|0.000000|3000.000000|1440.000000|   0.000000|   0.000000| 900.000000|
|G_006|     US|    25.01.2024|1400.000000|   0.000000|0.000000|   0.000000|1440.000000|   0.000000|   0.000000| 900.000000|
|G_007|     US|    25.01.2024|1400.000000|   0.000000|0.000000|   0.000000|1440.000000|   0.000000|   0.000000| 900.000000|
+-----+-------+--------------+-----------+-----------+--------+-----------+-----------+-----------+-----------+-----------+