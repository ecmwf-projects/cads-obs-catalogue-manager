from pathlib import Path

import numpy
import yaml

from cdsobs.cdm.lite import cdm_lite_variables
from cdsobs.service_definition.api import get_service_definition
from cdsobs.service_definition.service_definition_models import Description


def main():
    dataset_name = "insitu-comprehensive-upper-air-observation-network"
    all_cdm_lite_variables = [
        v for section in cdm_lite_variables for v in cdm_lite_variables[section]
    ]
    vardict = numpy.load("dic_type_attributes.npy", allow_pickle=True).item()
    service_definition = get_service_definition(dataset_name)
    descriptions = service_definition.sources["CUON"].descriptions

    for name, description in descriptions.items():
        description.long_name = description.long_name.replace("_", " ").title()

    for table_name, table_vars in vardict.items():
        for v, vattrs in table_vars.items():
            if v in all_cdm_lite_variables:
                description = vattrs["description"]
                dtype = vattrs["type"].__name__
                if dtype == "bytes_":
                    dtype = "object"
                long_name = v.replace("_", " ").title()
                descriptions[v] = Description(
                    description=description, dtype=dtype, long_name=long_name
                )

    output_path = Path("service_definition.yml")
    with output_path.open("w") as op:
        op.write(yaml.dump(service_definition.dict()))


if __name__ == "__main__":
    main()
