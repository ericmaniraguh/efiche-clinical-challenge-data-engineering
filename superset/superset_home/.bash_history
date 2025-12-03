# Disable async SQL Lab queries (avoid worker issues)
SQLLAB_ASYNC = False
SQLLAB_ASYNC_TIME_LIMIT_SEC = None
docker exec -it efiche_superset bash
exir
exit
cd /app/superset_home
nano superset_config.py
exit
