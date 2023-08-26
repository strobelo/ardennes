from pydantic import BaseModel
import pickle

class SeralizationHandler():
    def serialize(self, model: BaseModel) -> bytes:
        return pickle.dumps(model)

    def deserialize(self, serial: bytes) -> BaseModel:
        return pickle.loads(serial)
