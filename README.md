# celery-clusterwatch

Utility for reporting Celery cluster statistics to AWS CloudWatch Metrics

    

## Running

    $ celery clusterwatch \
        --broker=redis://localhost:6379 \
        --frequency=60 \
        --region=eu-central-1 \
        --access_key=access_key \
        --secret_key=secret_key
