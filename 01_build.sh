#!/bin/bash
docker run --rm\
            -v `pwd`:/src\
            -w /src\
            sptkl/docker-geosupport:19d bash -c '
                        pip install -r requirements.txt; 
                        python3 python/build.py; 
                        python3 python/format.py'