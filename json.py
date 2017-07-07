#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Validating JSON using Phython Code
import json
str_json = '{"employees":[{"firstName":"John", "lastName":"Doe"},{"firstName":"Anna", "lastName":"Smith"},{"firstName":"Peter", "lastName":"Jones"}]}'

data_json = json.loads(str_json)

print(data_json)
