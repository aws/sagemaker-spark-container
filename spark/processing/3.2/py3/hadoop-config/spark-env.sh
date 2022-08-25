#EMPTY FILE AVOID OVERRIDDING ENV VARS
# Specifically, without copying the empty file, SPARK_HISTORY_OPTS will be overriden, 
# spark.history.ui.port defaults to 18082, and spark.eventLog.dir defaults to local fs
