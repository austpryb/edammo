# Standard
from __future__ import print_function
import os
import json
import logging
from python_fraxses_wrapper.wrapper import FraxsesWrapper
from dataclasses import dataclass
from python_fraxses_wrapper.error import WrapperError
from python_fraxses_wrapper.response import Response, ResponseResult
from local_settings import * 
import requests

logging.basicConfig(level=logging.DEBUG)

TOPIC_NAME = "edammo_request"

@dataclass
class PredictionParameters:
    data: str

@dataclass
class FraxsesPayload:
    id: str
    obfuscate: bool
    payload: PredictionParameters


def handle_message(message):
    try:
        data = message.payload
        try:
            data = data.payload['data']
        except Exception as e:
            return "Error in payload parsing 'data' element, " + str(e) 
        try:
            # if authenticated already just pass to the next step
            # auth = someAuthFunc()
            pass
        except Exception as e:
            pass
        ########################### PARSE METHOD FROM THIS BLOCK #########################
        try:
            method = data['method'] # AUTH, PREDICT, TRAIN, ETC.
        except Exception as e:
            return "Error in payload parsing 'method' element, " + str(e)
        ########################### BUSINESS LOGIC HERE ##################################
        if method == 'PREDICT':
            # features = data['features']
            # return the results of a prediction
            pass
        elif method == 'TRAIN':
            # samples = data['sample_location']
            # do some training on the samples
            pass 
        ############################# IF ANYTHING FAILS THROW THIS EXCEPTION ############
    except Exception as e:
        print("Error in wrapper parsing", str(e))
        return str(e)
    return method

if __name__ == "__main__":
    wrapper = FraxsesWrapper(group_id="test", topic=TOPIC_NAME) 

    with wrapper as w:
        for message in wrapper.receive(FraxsesPayload):
            if type(message) is not WrapperError:
                task = handle_message(message)
                response = Response(
                    result=ResponseResult(success=True, error=""),
                    payload=task,
                )
                message.respond(response)
                print("Successfully responded to coordinator")
            else:
                error = message
                print("Error in wrapper", error.format_error(), message)
                error.log()


