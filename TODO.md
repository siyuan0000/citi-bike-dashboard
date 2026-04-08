# Project TODO List

## Lab 4: Website and DB (AWS Elastic Beanstalk & RDS)
- [completed] Incorporate SQLAlchemy and establish connection logic to the AWS RDS database.
- [completed] Create a periodic task or an endpoint to routinely save the current Citi Bike GBFS data into the RDS database.
- [completed] Verify execution configuration inside Elastic Beanstalk (`.ebextensions` and environment variables).

## Lab 5: Spark Integration (S3 & EMR)
- [ ] Configure `boto3` for integration with Amazon S3.
- [ ] Write integration API in `application.py` allowing the frontend to extract Spark-processed answers directly from the S3 bucket.
- [ ] Update `history.html` template to render the data distributions dynamically from the `api/history-data` endpoint.
- [ ] Set up Spark computation scripts representing the backend data transformation pipeline (to be submitted to EMR).
