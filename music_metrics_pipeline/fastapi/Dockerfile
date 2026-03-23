FROM public.ecr.aws/lambda/python:3.12

COPY app/__init__.py ${LAMBDA_TASK_ROOT}/app/
COPY app/config.py ${LAMBDA_TASK_ROOT}/app/
COPY app/models.py ${LAMBDA_TASK_ROOT}/app/
COPY app/utils/__init__.py ${LAMBDA_TASK_ROOT}/app/utils/
COPY app/utils/s3_keys.py ${LAMBDA_TASK_ROOT}/app/utils/
COPY app/utils/s3_io.py ${LAMBDA_TASK_ROOT}/app/utils/

COPY music_metrics_pipeline/fastapi/main.py ${LAMBDA_TASK_ROOT}/
COPY music_metrics_pipeline/fastapi/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

CMD ["main.handler"]