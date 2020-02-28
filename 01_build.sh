#!/bin/bash
docker run -it --rm\
            -v `pwd`:/src\
            -w /src\
            sptkl/docker-geosupport:20.1.0 bash -c '
                        pip install -r requirements.txt; 
                        python3 python/build.py; 
                        python3 python/create_corrections.py;
                        python3 python/format.py'