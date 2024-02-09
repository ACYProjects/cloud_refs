#!/usr/bin/env python

# Copyright 2019 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import base64
import json
import logging
import os
import random
import requests
import sys
import time
import traceback

logger = logging.getLogger(os.getenv('FUNCTION_NAME'))
#logger.setLevel(logging.INFO)
logger.setLevel(logging.DEBUG)

# widget status api-endpoint configuration
api_config = {"GCP_REGION" :  os.getenv('FUNCTION_REGION'), 
              "PROJECT_ID" : os.getenv('GCP_PROJECT') }

WIDGET_STATUS_URL = "https://{GCP_REGION}-{PROJECT_ID}.cloudfunctions.net/function-widget_status".format(**api_config)
  

def process_widget_from_pubsub(data, context):
    """Background Cloud Function to be triggered by Pub/Sub.
    Args:
         msg_data (dict): The dictionary with data specific to this type of event.
         context (google.cloud.functions.Context): The Cloud Functions event
         metadata.
    """

    try: 

        # parse widget from message

        logger.debug("data : {}".format(repr(data))) 

        if 'data' in data:
            data_json = base64.b64decode(data['data']).decode('utf-8')
        else:
            raise Exception("data was not provided");
    
        data_context = json.loads(data_json)

        logger.debug("data_context : {}".format(repr(data_context))) 

        def extract_data (data_label) :
            if data_label in data_context:
                return data_context[data_label]
            else :
                raise KeyError("{} was not provided".format(data_label));

        widget_uid = extract_data('widget_uid')

        try : 
            widget_content = extract_data('widget_content')
        except KeyError :
           r = requests.post(url=WIDGET_STATUS_URL, 
                             json={"widget_uid" : widget_uid, 
                                   "widget_status_code":"ERROR"})
           logger.info("Setting {}:ERROR ".format(widget_uid))

           # if no widget_content is provided, allow message to be pulled from queue 
           return

        #####  process widgets
        api_params = {"widget_uid" : widget_uid} 

        #insert widget record in MySQL: status = processing
        r = requests.post(url=WIDGET_STATUS_URL, json=api_params)
        
        logger.info("Processing {}:{} ".format(widget_uid, widget_content) )

        # simulate long (and variable) processing time
        widget_processing_time = random.random() * 20
        time.sleep(widget_processing_time)

        #update widget record in MySQL: status = complete
        r = requests.put(url=WIDGET_STATUS_URL, json=api_params)

    except :
        exc_type, exc_value, exc_traceback = sys.exc_info()
        
        logging.error(repr(traceback.format_exception(exc_type, exc_value, exc_traceback)))



