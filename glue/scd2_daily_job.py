"""Script that the Glue job runs (uploaded to S3 by Terraform).

The pipeline code ships as --extra-py-files (dist/cdc_pipeline.zip, built by
scripts/package_glue.sh), so this file only hands over to it.
"""
import sys

from pipeline.glue_job import main

sys.exit(main())
