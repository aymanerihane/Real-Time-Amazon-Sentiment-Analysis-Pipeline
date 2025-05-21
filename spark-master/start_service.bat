@echo off
echo Starting Spark sentiment analysis service...
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 /ml_service.py
IF %ERRORLEVEL% NEQ 0 (
    echo Error: Service process failed with exit code %ERRORLEVEL%
) ELSE (
    echo Service terminated successfully.
)
echo.
echo Press any key to exit...
pause > nul