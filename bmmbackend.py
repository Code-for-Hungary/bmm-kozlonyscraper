import requests
import logging

class bmmbackend:
    
    def __init__(self, backendURL, generatorID) -> None:

        if backendURL.endswith('/'):
            self.backendURL = backendURL[:-1]
        else:
            self.backendURL = backendURL

        self.generatorID = generatorID

    def getEvents(self, api_key):

        try:
            response = requests.get(f"{self.backendURL}/api/events/bygenerator/{self.generatorID}?api_key={api_key}")
            response = response.json()
            return response
        except Exception as e:
            logging.exception('Az eseményeket nem tudom lekérdezni a backendtől.')
            raise e
    
    def notifyEvent(self, eventUUID, content, api_key):

        notificationData = {
            'uuid': self.generatorID,
            'eventUuid': eventUUID,
            'content': content
        }
        try:
            response = requests.post(f"{self.backendURL}/api/events/notify/{eventUUID}?api_key={api_key}", data=notificationData)
            return response
        except Exception as e:
            logging.exception('A backend értesítése nem sikerült.')
            raise e