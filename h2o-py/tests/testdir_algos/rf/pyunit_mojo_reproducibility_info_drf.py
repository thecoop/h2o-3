import sys

sys.path.insert(1, "../../../")
from tests import pyunit_utils
from random import randint

def drf_mojo_reproducibility_info():
    problems = ['binomial', 'multinomial', 'regression']
    PROBLEM = problems[randint(0, (len(problems) - 1))]
    TESTROWS = 2000
    df = pyunit_utils.random_dataset(PROBLEM, verbose=False, NTESTROWS=TESTROWS)
    train = df[TESTROWS:, :]
    x = list(set(df.names) - {"respose"})
    params = {'ntrees': 50, 'max_depth': 4}

    drfModel = pyunit_utils.build_save_model_DRF(params, x, train, "response")

    isinstance(drfModel._model_json['output']['reproducibility_information_map']['cluster configuration']['H2O cluster uptime'], int)
    isinstance(drfModel._model_json['output']['reproducibility_information_map']['node information']['Node 0']['java_version'], str)
    isinstance(drfModel._model_json['output']['reproducibility_information_map']['input frames information']['training_frame_checksum'], int)   


if __name__ == "__main__":
    pyunit_utils.standalone_test(drf_mojo_reproducibility_info)
else:
    drf_mojo_reproducibility_info()
