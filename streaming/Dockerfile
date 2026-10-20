FROM apache/spark:3.5.5
USER root
# The official image runs Spark as UID 185. Ivy downloads Kafka connector
# packages at startup and must be able to write its cache and resolved POMs.
# Without these directories, spark-submit crashes before streaming starts.
RUN pip install --no-cache-dir 'psycopg[binary]==3.2.6' \
 && mkdir -p /opt/spark/checkpoints /home/spark/.ivy2/cache /home/spark/.ivy2/jars \
 && chown -R 185:0 /opt/spark/checkpoints /home/spark/.ivy2
USER 185
ENV PYTHONPATH=/opt/spark/work-dir
