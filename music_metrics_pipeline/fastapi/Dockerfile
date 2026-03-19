# Use AWS's official Lambda Python base image
FROM public.ecr.aws/lambda/python:3.12

# Copy your app utilities and models
COPY app/ ${LAMBDA_TASK_ROOT}/app/

# Copy your FastAPI app
COPY music_metrics_pipeline/fastapi/main.py ${LAMBDA_TASK_ROOT}/

# Install dependencies
COPY music_metrics_pipeline/fastapi/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install -r requirements.txt --target "${LAMBDA_TASK_ROOT}"

# Lambda handler — this is what AWS calls
CMD ["main.handler"]