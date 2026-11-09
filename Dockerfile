# JupyterLab on top of the official Spark image, so the notebook (driver)
# runs the exact same Spark, Java and Python versions as the workers.
FROM apache/spark:4.0.0

USER root

RUN pip3 install --no-cache-dir \
      jupyterlab==4.2.5 \
      pandas==2.2.3 \
      pyarrow==17.0.0 \
    # Make `import pyspark` work using the Spark already in the image
    && SITE="$(python3 -c 'import site; print(site.getsitepackages()[0])')" \
    && echo "/opt/spark/python" > "$SITE/spark.pth" \
    && ls /opt/spark/python/lib/py4j-*-src.zip >> "$SITE/spark.pth" \
    # Jupyter needs a writable home directory
    && mkdir -p /home/spark && chown spark:spark /home/spark

ENV HOME=/home/spark \
    PYSPARK_PYTHON=python3 \
    PATH="/opt/spark/bin:${PATH}"

USER spark
WORKDIR /opt/spark/work-dir

EXPOSE 8888 4040

CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--ServerApp.token=", "--ServerApp.password="]
