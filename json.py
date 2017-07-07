#!/usr/bin/env python
# -*- coding: utf-8 -*-

#Validating JSON using Phython Code
import unicodedata
import simplejson as json



str_json = '{"employees":[{"firstName":"John", "lastName":"Doe"},{"firstName":"Anna", "lastName":"Smith"},{"firstName":"Peter", "lastName":"Jones"}]}'

data = json.loads(str_json)

for value in data['employees']:
    print value['lastName']
