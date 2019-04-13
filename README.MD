# S3-71

S3-71 is a lambda based solution for copying objects from one S3 Bucket to another. 

The project utilizes SQS queues and Lambda functions to run hundreds of parallel copying jobs, achieving high speed and reliability.

# Notes

S3-71 uses 2 lambda functions and 2 corresponding SQS queues to:
* List the objects in the S3 Bucket
* Copy the objects one-by-one from the source to destination buckets

It gets its speed by doing both task simultaneously and in large parallel proceseses.

# Results

S3-71 moved 1 million small (14 Byte) files from us-west-1 to us-east-2 in 12 minutes. This is an end-2-end timing, which included the listing of all objects in the bucket.

# Installation

Pre-requisites: Serverless Framework, Python 3.7 (may work with 3.6)

    $ ./intall.sh
    
# Run

    $ python copy_bucket -s source_buckey -d destination_bucket
    
# Caveats

* Default lambda size is 128MB, and any object greater than 120MB might not be transferred, increase the memory of the lambda function to cater for files up to ~3GB
* Currently will not copy across keys *beginning* with non-ascii utf-8 characters. It'll work if the non-ascii characters are part of the name
* Any object whose key contains characters that are *not* from the list below will fail:
    * \#x9, \#xA, #xD
    * \#x20 to #xD7FF
    * \#xE000 to #xFFFD
    * \#x10000 to #x10FFFF

# To-Do

* Test out s3.copy_object which amy get around the memory limit of lambda
* Support the copying of keys containing non-ascii utf-8 characters
* Output full list of objects -- somewhere

# The Name

I usually name my serverless project after radioactive elements, but I made an exception for this.

S3-71 is a homage to the SR-71, more commonly known as the Blackbird. A plane built in 1964, yet still holds the record for the fastest air-breathing manned aircract ever built!
