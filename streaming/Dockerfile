FROM apache/spark:3.5.5
USER root
RUN pip install --no-cache-dir 'psycopg[binary]==3.2.6' \
 && mkdir -p /opt/spark/checkpoints \
 && chown -R 185:0 /opt/spark/checkpoints
USER 185
ENV PYTHONPATH=/opt/spark/work-dir
