from modelstar import modelstar_read_path
from sklearn
import pickle


MODEL_FILE = modelstar_read_path(
    local_path='/Users/adithya/projects/modelstar-org/test/test-project/models/model.pkl')


def model_inf(tv: float, radio: float, newspaper: float) -> float:

    with open(MODEL_FILE, 'rb') as file:
        saved_model = pickle.load(file)

    y_inf = saved_model.predict([[tv, radio, newspaper]])

    return float(y_inf[0])


if __name__ == '__main__':
    # how this function is used in local

    y = model_inf(230.1, 37.8, 69.2)
    print(y)
