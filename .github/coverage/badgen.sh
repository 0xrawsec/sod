#!/bin/bash

url_message=`cat | tail -n -1 | awk -F"\t" '{print $NF}' | tr -d '[:cntrl:]' | sed 's/%/%25/'`
curl -s https://img.shields.io/badge/coverage-${url_message}-informational