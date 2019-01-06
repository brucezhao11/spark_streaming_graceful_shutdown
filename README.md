# spark_streaming_graceful_shutdown

This script is used to do graceful shutdown for Spark streaming applications running on AWS EMR automatically.


### HOW TO USE?

1. Run it on an AWS EC2 instance.
2. Pepare pem file on the instance, rename it to "pub-for-team.pem". The pem file is used to access your EMR instances.
3. Command:
python graceful_shutdown.py --cluster <cluster name> --app <spark_application_name> --region <aws region> --flag <identifier>
e.g.,
/usr/local/bin/python graceful_shutdown.py --cluster stg-lfs-an --app stg-lfs-an-mfnn6 --region ap-northeast-1 --flag mfnn6

