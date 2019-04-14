# S3-71

S3-71 is a lambda based solution for copying objects from one S3 Bucket to another. 

The project utilizes Lambda functions to run hundreds of parallel copying jobs, achieving high speeds.

# Notes

S3-71 uses 2 lambda functions and 2 corresponding SQS queues to:
* List the objects in the S3 Bucket
* Copy the objects one-by-one from the source to destination buckets

It achieves its speed by doing both task simultaneously and in large parallel proceseses. Capable of ~500 copying processes at once.

SQS queues are used to ensure the speeds are achieved via ramping up, and to catch exceptions via a Dead-Letter-Que.

# Results

Total time to move 1 million 14-Byte files between 2 buckets in a single region in 12 minutes. 

Your mileage will vary depending on latency between regions, size of your files, size of lambda function, etc etc.

# Installation

Pre-requisites: [Serverless Framework](https://serverless.com/), Python 3.7 (may work with 3.6)

    $ ./intall.sh

A `--max_lambda_size_MB` argument can be set to handle larger files (from 128 to 3008 in increments of 64)

# Run

    (venv) $ python copy_bucket.py -s source_buckey -d destination_bucket
    
# Caveats

* Default lambda size is 128MB, and any object greater than ~110MB will not be transferred, increase the memory of the lambda function by setting the `--max_lambda_size_MB` parameter during install to cater for files up to ~3GB
* Currently will not copy across keys *beginning* with non-ascii utf-8 characters. Will work if the non-ascii character is part of the name.
* Any object whose key contains characters that are *not* from the list below will fail (sqs limit):
    * \#x9, \#xA, #xD
    * \#x20 to #xD7FF
    * \#xE000 to #xFFFD
    * \#x10000 to #x10FFFF

# To-Do

* Test out s3.copy_object which amy get around the memory limit of lambda
* Support the copying of keys beginning with non-ascii utf-8 characters
* Output full list of objects -- somewhere

# The Name

I usually name my serverless project after radioactive elements, but I made an exception for this.

S3-71 is a homage to the SR-71, more commonly known as the Blackbird, the fastest plane ever built.
