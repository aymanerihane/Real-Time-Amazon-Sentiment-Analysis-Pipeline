@echo off
docker exec spark-worker-2 pip install textblob   
docker exec spark-worker-1 pip install textblob
echo Starting Spark model training process...
docker exec -it spark-master /opt/bitnami/spark/bin/spark-submit --master spark://spark-master:7077 --deploy-mode client /train_model.py
IF %ERRORLEVEL% NEQ 0 (
    echo Error: Training process failed with exit code %ERRORLEVEL%
) ELSE (
    echo Training process completed successfully.
)
echo.
echo Press any key to exit...
pause > nul